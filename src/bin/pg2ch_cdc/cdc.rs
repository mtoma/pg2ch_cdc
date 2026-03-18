//! CDC replication — LSN-target approach with multi-table routing.
//!
//! Before starting the replication stream, we snapshot pg_current_wal_flush_lsn()
//! as our target. We stream WAL until we reach that LSN, apply all changes to
//! the correct ClickHouse tables, then confirm the position and exit.
//! Any WAL accumulated during processing is left for the next run.

use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use pg2ch_cdc::clickhouse::{CdcBatch, ChClient};
use pg2ch_cdc::pg::PgClient;
use pg2ch_cdc::pgoutput::{decode_pgoutput, PgoutputMessage, RelationInfo};
use pg2ch_cdc::types::{build_delete_row, tuple_to_strings};

// ── Standby status feedback ─────────────────────────────────────────────

fn build_standby_status(lsn: u64) -> Vec<u8> {
    const PG_EPOCH_OFFSET: u64 = 946_684_800;
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
        - PG_EPOCH_OFFSET * 1_000_000;

    let mut msg = Vec::with_capacity(34);
    msg.push(b'r');
    msg.extend_from_slice(&lsn.to_be_bytes()); // written
    msg.extend_from_slice(&lsn.to_be_bytes()); // flushed
    msg.extend_from_slice(&lsn.to_be_bytes()); // applied
    msg.extend_from_slice(&now_us.to_be_bytes()); // client time
    msg.push(0);
    msg
}

// ── poll helper ─────────────────────────────────────────────────────────

fn wait_for_data(fd: i32, timeout: Duration) {
    let mut poll_fd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    unsafe {
        libc::poll(
            &mut poll_fd as *mut libc::pollfd,
            1,
            timeout.as_millis() as libc::c_int,
        );
    }
}

/// Parse a PostgreSQL LSN string like "3C7D/2F652920" into a u64.
fn parse_lsn(s: &str) -> Result<u64> {
    let s = s.trim();
    let (hi, lo) = s.split_once('/').with_context(|| format!("Invalid LSN format: '{}'", s))?;
    let hi = u64::from_str_radix(hi, 16).with_context(|| format!("Invalid LSN high part: '{}'", hi))?;
    let lo = u64::from_str_radix(lo, 16).with_context(|| format!("Invalid LSN low part: '{}'", lo))?;
    Ok((hi << 32) | lo)
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

/// Parse TRUNCATE message to extract relation IDs.
/// Format: u32 n_relations, u8 options, then n_relations × u32 rel_id.
fn parse_truncate_rel_ids(data: &[u8]) -> Vec<u32> {
    let mut ids = Vec::new();
    if data.len() < 5 { return ids; } // need at least n_relations(4) + options(1)
    let n = u32::from_be_bytes(data[0..4].try_into().unwrap_or([0; 4]));
    // skip options byte at data[4]
    let mut pos = 5;
    for _ in 0..n {
        if pos + 4 > data.len() { break; }
        let id = u32::from_be_bytes(data[pos..pos+4].try_into().unwrap_or([0; 4]));
        ids.push(id);
        pos += 4;
    }
    ids
}

// ── CDC configuration ───────────────────────────────────────────────────

pub struct CdcConfig {
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
    pub ch_host: String,
    pub ch_port: u16,
    pub ch_user: String,
    pub ch_password: String,
    pub slot: String,
    pub publication: String,
    /// PG table name → CH table name (e.g. "csco_ipcd" → "db_providers_2.csco_ipcd")
    pub tables: Vec<(String, String)>,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub binary: bool,
    pub ch_timeout_secs: u64,
    /// Pre-snapshotted target LSN (from before initial loads).
    /// If set, CDC uses this instead of querying pg_current_wal_flush_lsn().
    pub target_lsn: Option<String>,
}

// ── Multi-table message processing ──────────────────────────────────────

fn process_message(
    msg: PgoutputMessage,
    relations: &mut HashMap<u32, RelationInfo>,
    rel_to_table: &mut HashMap<u32, String>,
    batches: &mut HashMap<String, CdcBatch>,
    expected_oids: &HashMap<String, u32>,
    stale_rel_ids: &mut HashSet<u32>,
) -> Result<()> {
    match msg {
        PgoutputMessage::Relation(rel) => {
            if batches.contains_key(&rel.name) {
                let ident_char = rel.replica_identity as char;
                let ident_desc = match rel.replica_identity {
                    b'd' => "default (PK)",
                    b'f' => "FULL",
                    b'n' => "NOTHING",
                    b'i' => "INDEX",
                    _ => "unknown",
                };
                if rel.replica_identity == b'n' {
                    error!(
                        "REPLICA IDENTITY NOTHING on {}.{} (id={}) — PK was dropped! \
                         DELETEs/UPDATEs will be lost. Table needs full reload after PK is restored.",
                        rel.namespace, rel.name, rel.id
                    );
                }

                // Check if this rel_id matches the expected OID stored in CH.
                // If not, this is WAL from the OLD (dropped) table — mark as stale.
                let expected = expected_oids.get(&rel.name).copied().unwrap_or(0);
                if expected != 0 && rel.id != expected {
                    error!(
                        "STALE OID: {}.{} has rel_id={} but CH has _pg2ch_rel_id={}. \
                         Table was dropped+recreated. Ignoring WAL for old OID.",
                        rel.namespace, rel.name, rel.id, expected
                    );
                    stale_rel_ids.insert(rel.id);
                } else {
                    info!(
                        "Relation: {}.{} (id={}, {} columns, replica_identity={} [{}]) — matched to CDC batch",
                        rel.namespace, rel.name, rel.id, rel.columns.len(), ident_char, ident_desc
                    );
                    // Update the batch's rel_id from the WAL Relation message
                    if let Some(batch) = batches.get_mut(&rel.name) {
                        batch.set_rel_id(rel.id);
                    }
                }
            } else {
                debug!(
                    "Relation: {}.{} (id={}, {} columns)",
                    rel.namespace, rel.name, rel.id, rel.columns.len()
                );
            }
            rel_to_table.insert(rel.id, rel.name.clone());
            relations.insert(rel.id, rel);
        }
        PgoutputMessage::Insert { rel_id, values } => {
            if stale_rel_ids.contains(&rel_id) {
                debug!("Skipping INSERT for stale rel_id {}", rel_id);
                return Ok(());
            }
            let rel = relations.get(&rel_id).with_context(|| {
                format!("INSERT for unknown relation id {}", rel_id)
            })?;
            let table_name = rel_to_table.get(&rel_id).with_context(|| {
                format!("INSERT: no table mapping for relation id {}", rel_id)
            })?;
            if let Some(batch) = batches.get_mut(table_name) {
                let row = tuple_to_strings(&values, rel);
                batch.add_insert(row);
                debug!("INSERT into {}: {} values", table_name, values.len());
            } else {
                warn!("INSERT for table '{}' which has no batch — skipping", table_name);
            }
        }
        PgoutputMessage::Update {
            rel_id, old_values, new_values,
        } => {
            if stale_rel_ids.contains(&rel_id) {
                debug!("Skipping UPDATE for stale rel_id {}", rel_id);
                return Ok(());
            }
            let rel = relations.get(&rel_id).with_context(|| {
                format!("UPDATE for unknown relation id {}", rel_id)
            })?;
            let table_name = rel_to_table.get(&rel_id).with_context(|| {
                format!("UPDATE: no table mapping for relation id {}", rel_id)
            })?;
            if let Some(batch) = batches.get_mut(table_name) {
                // When PK columns change, pgoutput sends old key in old_values (flag K).
                // We must delete the old PK before inserting the new row, otherwise the
                // old PK identity becomes a phantom row in ReplacingMergeTree.
                if let Some(ref old_vals) = old_values {
                    let old_row = build_delete_row(old_vals, rel);
                    batch.add_delete(old_row);
                    debug!("UPDATE {}: PK changed — deleting old key", table_name);
                }
                let row = tuple_to_strings(&new_values, rel);
                batch.add_update(row);
                debug!("UPDATE {}: {} values", table_name, new_values.len());
            } else {
                warn!("UPDATE for table '{}' which has no batch — skipping", table_name);
            }
        }
        PgoutputMessage::Delete {
            rel_id,
            key_or_old,
            values,
        } => {
            if stale_rel_ids.contains(&rel_id) {
                debug!("Skipping DELETE for stale rel_id {}", rel_id);
                return Ok(());
            }
            let rel = relations.get(&rel_id).with_context(|| {
                format!("DELETE for unknown relation id {}", rel_id)
            })?;
            let table_name = rel_to_table.get(&rel_id).with_context(|| {
                format!("DELETE: no table mapping for relation id {}", rel_id)
            })?;
            if let Some(batch) = batches.get_mut(table_name) {
                let row = if key_or_old == b'K' {
                    build_delete_row(&values, rel)
                } else {
                    tuple_to_strings(&values, rel)
                };
                batch.add_delete(row);
                debug!("DELETE from {}", table_name);
            } else {
                warn!("DELETE for table '{}' which has no batch — skipping", table_name);
            }
        }
        PgoutputMessage::Begin { .. } | PgoutputMessage::Commit => {}
    }
    Ok(())
}

// ── Drain CDC with LSN target ───────────────────────────────────────────

/// Process WAL from the slot's confirmed position up to a target LSN.
///
/// 1. Snapshot pg_current_wal_flush_lsn() as the target
/// 2. Start replication stream
/// 3. Read WAL messages, route to per-table batches
/// 4. Stop when WALStart reaches the target LSN
/// 5. Flush all batches, confirm the LSN we actually processed
///
/// Any WAL accumulated during processing is left for the next run.
pub fn drain_cdc(cfg: &CdcConfig) -> Result<u64> {
    // ── 1. Determine target LSN ─────────────────────────────────────────
    let pg = PgClient::connect(&cfg.pg_host, cfg.pg_port, &cfg.pg_database, &cfg.pg_user, &cfg.pg_password)?;

    let target_lsn = if let Some(ref lsn_str) = cfg.target_lsn {
        let lsn = parse_lsn(lsn_str)?;
        info!("Using pre-snapshotted target LSN: {}", format_lsn(lsn));
        lsn
    } else {
        let rows = pg.query("SELECT pg_current_wal_flush_lsn()::text")?;
        parse_lsn(&rows[0][0])?
    };

    let slot_rows = pg.query(&format!(
        "SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name = '{}'",
        cfg.slot
    ))?;
    let confirmed_lsn = parse_lsn(&slot_rows[0][0])?;
    let restart_lsn = parse_lsn(&slot_rows[0][1])?;

    if confirmed_lsn >= target_lsn {
        info!("Slot already at {} — nothing to process", format_lsn(confirmed_lsn));
        return Ok(0);
    }

    let gap_gb = (target_lsn - confirmed_lsn) as f64 / 1_073_741_824.0;
    let replay_gb = (confirmed_lsn - restart_lsn) as f64 / 1_073_741_824.0;

    info!(
        "CDC target: process WAL from {} to {} ({:.2} GB, {:.2} GB pre-confirmed replay from {})",
        format_lsn(confirmed_lsn),
        format_lsn(target_lsn),
        gap_gb,
        replay_gb,
        format_lsn(restart_lsn)
    );
    // Keep pg alive — used for pg_stat_replication progress queries during CDC

    // ── 2. Create ChClient and per-table batches ────────────────────────
    let ch = ChClient::new(&cfg.ch_host, cfg.ch_port, &cfg.ch_user, &cfg.ch_password, cfg.ch_timeout_secs);

    let mut batches: HashMap<String, CdcBatch> = HashMap::new();
    for (pg_table, ch_table) in &cfg.tables {
        let (ch_db, ch_tbl) = ch_table
            .split_once('.')
            .with_context(|| format!("ch_table must be db.table format: {}", ch_table))?;

        let col_response = ch.query(&format!(
            "SELECT name FROM system.columns \
             WHERE database = '{}' AND table = '{}' \
             AND name NOT LIKE '_pg2ch_%' ORDER BY position FORMAT TabSeparated",
            ch_db, ch_tbl
        ))?;
        let ch_columns: Vec<String> = col_response
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.to_string())
            .collect();

        if ch_columns.is_empty() {
            warn!("No columns found for {} — skipping CDC for this table", ch_table);
            continue;
        }

        debug!("CDC target: {} → {} ({} columns)", pg_table, ch_table, ch_columns.len());
        batches.insert(
            pg_table.clone(),
            CdcBatch::new(ch_table.clone(), ch_columns, cfg.batch_size, cfg.flush_interval),
        );
    }

    if batches.is_empty() {
        bail!("No valid CDC target tables found");
    }

    // Query expected OIDs from CH — used to filter stale WAL from dropped+recreated tables.
    // If a table has multiple distinct _pg2ch_rel_id values, data is corrupted → abort.
    let mut expected_oids: HashMap<String, u32> = HashMap::new();
    for (pg_table, ch_table) in &cfg.tables {
        let (ch_db, ch_tbl) = ch_table
            .split_once('.')
            .unwrap_or(("default", ch_table.as_str()));
        // Check if the column exists (backward compat for old tables)
        let has_col = ch.query(&format!(
            "SELECT count() FROM system.columns \
             WHERE database='{}' AND table='{}' AND name='_pg2ch_rel_id' \
             FORMAT TabSeparated",
            ch_db, ch_tbl
        ))?.trim() == "1";
        if has_col {
            let oids_str = ch.query(&format!(
                "SELECT DISTINCT _pg2ch_rel_id FROM {} SETTINGS final=0 FORMAT TabSeparated",
                ch_table
            ))?;
            let oids: Vec<u32> = oids_str.lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| l.trim().parse().ok())
                .filter(|&id| id != 0)
                .collect();
            if oids.len() > 1 {
                bail!(
                    "FATAL: {} has multiple _pg2ch_rel_id values: {:?}. \
                     Data from different table OIDs is mixed — table must be dropped and reloaded.",
                    ch_table, oids
                );
            }
            if oids.len() == 1 {
                expected_oids.insert(pg_table.clone(), oids[0]);
                debug!("Expected OID for {}: {}", pg_table, oids[0]);
            }
        }
        // If no column or no non-zero OID, expected_oids won't have an entry → accept all
    }

    let mut stale_rel_ids: HashSet<u32> = HashSet::new();

    info!("Prepared {} table batches for CDC", batches.len());

    // ── 3. Connect replication and start streaming ──────────────────────
    let dsn = format!(
        "host={} port={} dbname={} user={} password={} replication=database \
         keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3",
        cfg.pg_host, cfg.pg_port, cfg.pg_database, cfg.pg_user, cfg.pg_password
    );
    let conn = libpq::Connection::new(&dsn).context("Failed to connect to PostgreSQL (replication)")?;
    if conn.status() != libpq::connection::Status::Ok {
        bail!(
            "PostgreSQL replication connection failed: {}",
            conn.error_message().unwrap_or("unknown error")
        );
    }
    info!("Connected to PostgreSQL (replication mode)");

    let start_cmd = if cfg.binary {
        format!(
            "START_REPLICATION SLOT {} LOGICAL 0/0 \
             (proto_version '1', publication_names '{}', binary 'true')",
            cfg.slot, cfg.publication
        )
    } else {
        format!(
            "START_REPLICATION SLOT {} LOGICAL 0/0 \
             (proto_version '1', publication_names '{}')",
            cfg.slot, cfg.publication
        )
    };
    info!("Executing: {}", start_cmd);
    conn.send_query(&start_cmd)
        .context("Failed to send START_REPLICATION")?;

    let socket_fd = conn.socket().context("Failed to get socket fd")?;
    wait_for_data(socket_fd, Duration::from_secs(5));
    conn.consume_input().context("consume_input failed")?;

    if let Some(res) = conn.result() {
        info!("Replication stream status: {:?}", res.status());
    }

    // ── 4. Read WAL until we reach target_lsn ───────────────────────────
    let mut relations: HashMap<u32, RelationInfo> = HashMap::new();
    let mut rel_to_table: HashMap<u32, String> = HashMap::new();
    // Start with the confirmed position — used for keepalive replies before
    // we've processed any WAL. This tells PG "I'm alive" without advancing the slot.
    let mut last_processed_lsn: u64 = confirmed_lsn;
    let mut last_server_wal_end: u64 = 0; // tracks PG's decoding progress from keepalives
    let mut total_wal_msgs: u64 = 0;
    let mut last_progress = Instant::now();
    let mut last_feedback = Instant::now();
    let cdc_start = Instant::now();
    let _reached_target;

    loop {
        // Short poll to ensure we reply to keepalives within wal_sender_timeout (60s)
        wait_for_data(socket_fd, Duration::from_secs(5));
        conn.consume_input().context("consume_input failed")?;

        let mut got_data = false;
        loop {
            let data = match conn.copy_data(true) {
                Ok(data) => data,
                Err(e) => {
                    if conn.status() != libpq::connection::Status::Ok {
                        bail!("PostgreSQL connection error during copy_data: {}", e);
                    }
                    break;
                }
            };
            let raw = data.as_ref();
            if raw.is_empty() {
                break;
            }

            got_data = true;

            match raw[0] {
                b'w' if raw.len() >= 25 => {
                    let wal_start = u64::from_be_bytes(raw[1..9].try_into().unwrap());
                    let wal_end = u64::from_be_bytes(raw[9..17].try_into().unwrap());
                    let payload = &raw[25..];
                    last_processed_lsn = wal_end;
                    total_wal_msgs += 1;

                    match decode_pgoutput(payload) {
                        Some(msg) => {
                            process_message(msg, &mut relations, &mut rel_to_table, &mut batches, &expected_oids, &mut stale_rel_ids)?;
                        }
                        None => {
                            if !payload.is_empty() {
                                let msg_type = payload[0] as char;
                                match msg_type {
                                    'T' => {
                                        // TRUNCATE: parse relation IDs to log which tables
                                        let truncated = parse_truncate_rel_ids(&payload[1..]);
                                        let names: Vec<String> = truncated.iter()
                                            .map(|id| rel_to_table.get(id)
                                                .cloned()
                                                .unwrap_or_else(|| format!("rel_id={}", id)))
                                            .collect();
                                        warn!(
                                            "TRUNCATE received for {} table(s): {} — NOT HANDLED, CH data may diverge",
                                            truncated.len(), names.join(", ")
                                        );
                                    }
                                    'O' | 'Y' => {} // Origin, Type — safe to skip silently
                                    other => {
                                        warn!(
                                            "Unhandled pgoutput message type '{}' (0x{:02x}, {} bytes)",
                                            other, other as u8, payload.len()
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Check if we've reached our target
                    if wal_start >= target_lsn {
                        debug!("Reached target LSN at WALStart {}", format_lsn(wal_start));
                        break;
                    }
                }
                b'k' if raw.len() >= 18 => {
                    let server_wal_end = u64::from_be_bytes(raw[1..9].try_into().unwrap());
                    last_server_wal_end = server_wal_end;
                    let reply_requested = raw[17] != 0;

                    // Reply with our last processed LSN (never the server's position)
                    if reply_requested && last_processed_lsn > 0 {
                        conn.put_copy_data(&build_standby_status(last_processed_lsn))
                            .context("Failed to send standby status")?;
                        conn.flush().context("Failed to flush standby status")?;
                    }

                    // If the server's WAL end is at or past our target and it's sending
                    // keepalives (no more data to send), we've consumed everything up to target.
                    // Update last_processed_lsn so the outer loop's termination check triggers.
                    if server_wal_end >= target_lsn {
                        info!(
                            "Server WAL end {} >= target {} — all relevant WAL delivered",
                            format_lsn(server_wal_end),
                            format_lsn(target_lsn)
                        );
                        last_processed_lsn = target_lsn;
                        break;
                    }
                }
                _ => {
                    warn!("Unexpected copy data: type=0x{:02x}, len={}", raw[0], raw.len());
                }
            }
        }

        // Proactive standby status every 10 seconds — required to stay within
        // wal_sender_timeout (default 60s). All CDC tools do this (Debezium, PeerDB,
        // pglogrepl, pg_recvlogical). Without this, PG kills the connection during
        // long WAL scans where most data is for other tables.
        if last_feedback.elapsed() > Duration::from_secs(10) {
            conn.put_copy_data(&build_standby_status(last_processed_lsn))
                .context("Failed to send proactive standby status")?;
            conn.flush().context("Failed to flush proactive standby status")?;
            last_feedback = Instant::now();
        }

        // Flush any batch that's full or past its interval
        for batch in batches.values_mut() {
            if batch.should_flush() {
                batch.flush(&ch)?;
            }
        }

        // Progress logging every 10 seconds
        if last_progress.elapsed() > Duration::from_secs(10) {
            let total_applied: u64 = batches.values().map(|b| b.total_applied).sum();
            let total_ins: u64 = batches.values().map(|b| b.total_inserts).sum();
            let total_upd: u64 = batches.values().map(|b| b.total_updates).sum();
            let total_del: u64 = batches.values().map(|b| b.total_deletes).sum();
            let total_pending: usize = batches.values().map(|b| b.pending_count()).sum();

            // Query pg_stat_replication for the walsender's sent_lsn — this shows
            // how far PG has decoded, even when it sends no keepalives or XLogData.
            // Join via pg_stat_activity.query which contains the slot name, since
            // pg_stat_replication has no slot_name column (added in PG 17).
            let walsender_lsn = pg.query(&format!(
                "SELECT r.sent_lsn::text FROM pg_stat_replication r \
                 JOIN pg_stat_activity a ON r.pid = a.pid \
                 WHERE a.query LIKE '%\"{}\" LOGICAL%' OR a.query LIKE '% {} %'",
                cfg.slot, cfg.slot
            )).ok()
                .and_then(|rows| rows.into_iter().next())
                .and_then(|row| row.into_iter().next())
                .and_then(|lsn_str| parse_lsn(&lsn_str).ok());

            let walsender_sent = walsender_lsn.unwrap_or(0);

            let elapsed = cdc_start.elapsed().as_secs();
            let elapsed_str = if elapsed >= 3600 {
                format!("{}h{:02}m", elapsed / 3600, (elapsed % 3600) / 60)
            } else if elapsed >= 60 {
                format!("{}m{:02}s", elapsed / 60, elapsed % 60)
            } else {
                format!("{}s", elapsed)
            };

            // Throughput
            let msgs_per_sec = if elapsed > 0 { total_wal_msgs as f64 / elapsed as f64 } else { 0.0 };

            // Two-phase progress:
            // Phase 1 (replay): PG re-decodes from restart_lsn to confirmed_lsn — no data for us
            // Phase 2 (CDC):    PG sends new WAL from confirmed_lsn to target_lsn — actual changes
            let in_replay_phase = walsender_sent > 0 && walsender_sent < confirmed_lsn;
            let replay_total = confirmed_lsn.saturating_sub(restart_lsn);

            let (progress_str, state_info) = if in_replay_phase {
                // Phase 1: replay
                let replay_done = walsender_sent.saturating_sub(restart_lsn);
                let replay_pct = if replay_total > 0 {
                    (replay_done as f64 / replay_total as f64 * 100.0).min(100.0)
                } else {
                    0.0
                };
                let remaining_gb = confirmed_lsn.saturating_sub(walsender_sent) as f64 / 1_073_741_824.0;
                (
                    format!("replay {:.1}%", replay_pct),
                    format!(" [{:.2} GB to confirmed, then {:.2} GB CDC]", remaining_gb, gap_gb),
                )
            } else if walsender_sent == 0 && last_server_wal_end == 0 && total_wal_msgs == 0 {
                // No walsender visible yet
                (
                    "replay 0.0%".to_string(),
                    format!(" [waiting for PG walsender ({:.2} GB replay + {:.2} GB CDC)]", replay_gb, gap_gb),
                )
            } else {
                // Phase 2: CDC processing
                let effective_lsn = [walsender_lsn, Some(last_server_wal_end), Some(last_processed_lsn)]
                    .iter()
                    .filter_map(|x| *x)
                    .filter(|x| *x > 0)
                    .max()
                    .unwrap_or(last_processed_lsn);

                let cdc_pct = if target_lsn > confirmed_lsn && effective_lsn > confirmed_lsn {
                    ((effective_lsn - confirmed_lsn) as f64 / (target_lsn - confirmed_lsn) as f64 * 100.0).min(100.0)
                } else {
                    0.0
                };

                let state = if walsender_sent > last_processed_lsn && total_wal_msgs > 0 {
                    let buffered_gb = walsender_sent.saturating_sub(last_processed_lsn) as f64 / 1_073_741_824.0;
                    let remaining_gb = target_lsn.saturating_sub(walsender_sent) as f64 / 1_073_741_824.0;
                    format!(" [flushing {:.2} GB buffered, {:.2} GB remaining]", buffered_gb, remaining_gb)
                } else if effective_lsn > last_processed_lsn {
                    let remaining_gb = target_lsn.saturating_sub(effective_lsn) as f64 / 1_073_741_824.0;
                    format!(" [PG decoding: {:.2} GB remaining]", remaining_gb)
                } else {
                    String::new()
                };

                (format!("{:.1}%", cdc_pct), state)
            };

            info!(
                "CDC [{elapsed_str}] {} — {:.1}k msgs ({:.1}k/s, {}I/{}U/{}D){}",
                progress_str,
                total_wal_msgs as f64 / 1000.0,
                msgs_per_sec / 1000.0,
                total_ins, total_upd, total_del,
                state_info
            );
            last_progress = Instant::now();
        }

        // Check termination: have we reached the target?
        if last_processed_lsn >= target_lsn {
            _reached_target = true;
            break;
        }

        // If we got no data at all from the 30s poll, check if maybe the server
        // has scanned past our target without finding relevant changes
        if !got_data {
            // The server might be scanning WAL with no relevant changes for us.
            // Keep waiting — the server will eventually send a keepalive when it
            // reaches our target LSN range.
            debug!("No data received, waiting for server to scan WAL...");
        }
    }

    // ── 5. Final flush all batches ──────────────────────────────────────
    for batch in batches.values_mut() {
        batch.flush(&ch)?;
    }

    // ── 6. Confirm the LSN we processed ─────────────────────────────────
    let confirm_lsn = if last_processed_lsn > 0 {
        last_processed_lsn
    } else {
        // We processed no WAL messages but the server scanned past our target
        // (all WAL was for other tables). Confirm the target LSN.
        target_lsn
    };

    conn.put_copy_data(&build_standby_status(confirm_lsn))
        .context("Failed to send final standby status")?;
    conn.flush().context("Failed to flush final standby status")?;
    info!("Confirmed LSN: {}", format_lsn(confirm_lsn));

    // ── 7. Summary ──────────────────────────────────────────────────────
    let total_applied: u64 = batches.values().map(|b| b.total_applied).sum();
    let total_inserts: u64 = batches.values().map(|b| b.total_inserts).sum();
    let total_updates: u64 = batches.values().map(|b| b.total_updates).sum();
    let total_deletes: u64 = batches.values().map(|b| b.total_deletes).sum();

    if total_wal_msgs > 0 || total_applied > 0 {
        info!(
            "CDC complete: {} WAL messages, {} rows applied ({} inserts, {} updates, {} deletes)",
            total_wal_msgs, total_applied, total_inserts, total_updates, total_deletes
        );
        info!("{:<30} {:>10} {:>10} {:>10} {:>10}", "TABLE", "INSERTS", "UPDATES", "DELETES", "TOTAL");
        info!("{}", "─".repeat(75));
        let mut sorted_tables: Vec<_> = batches.iter().filter(|(_, b)| b.total_applied > 0).collect();
        sorted_tables.sort_by_key(|(name, _)| (*name).clone());
        for (pg_table, batch) in &sorted_tables {
            info!(
                "{:<30} {:>10} {:>10} {:>10} {:>10}",
                pg_table, batch.total_inserts, batch.total_updates, batch.total_deletes, batch.total_applied
            );
        }
        info!("{}", "─".repeat(75));
        info!(
            "{:<30} {:>10} {:>10} {:>10} {:>10}",
            "TOTAL", total_inserts, total_updates, total_deletes, total_applied
        );

        // Log relation IDs (= PG OIDs) for tables that had WAL activity.
        // Enables detecting DROP+RENAME by comparing OIDs across runs in historic logs.
        for (pg_table, _) in &sorted_tables {
            if let Some(rel) = relations.values().find(|r| &r.name == *pg_table) {
                info!("OID {}={}", pg_table, rel.id);
            }
        }
    } else {
        info!(
            "No relevant changes in WAL ({:.2} GB scanned, all for other tables/schemas)",
            (target_lsn - confirmed_lsn) as f64 / 1_073_741_824.0
        );
    }

    Ok(total_applied)
}
