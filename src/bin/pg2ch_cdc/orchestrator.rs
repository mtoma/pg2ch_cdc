//! Mirror orchestrator — idempotent, resilient, rsync-like.
//!
//! Inspects source (PG) and destination (CH), prints a clear diff of the
//! situation for each table, then takes the right action:
//!
//!   - CH table missing          → create it
//!   - CH empty, PG has rows     → initial load
//!   - CH partial load detected  → truncate + reload
//!   - CH has rows               → skip to CDC
//!   - Slot has pending WAL      → CDC will consume it
//!   - Nothing to do             → idle

use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn, error};

use crate::cdc::{CdcConfig, drain_cdc};
use pg2ch_cdc::clickhouse::{
    datetime_timezone, has_datetime, pin_datetime_timezone, ChClient,
};
use pg2ch_cdc::config::MirrorConfig;
use pg2ch_cdc::pg::PgClient;

pub fn run_mirror(config: &MirrorConfig) -> Result<()> {
    let src = &config.source;
    let dst = &config.destination;

    // ── Connect ─────────────────────────────────────────────────────────
    let pg = PgClient::connect(&src.host, src.port, &src.database, &src.user, &src.password)?;
    let ch = ChClient::new(
        &dst.host, dst.port, &dst.user, &dst.password,
        config.settings.ch_timeout_secs, &config.timezone,
    );

    // ── Validate the configured timezone against ClickHouse ─────────────
    validate_timezone_in_ch(&ch, &config.timezone, config.timezone_allow_dst)?;
    // The server default is what an unpinned DateTime column is stored on,
    // so we need it to judge pre-existing tables.
    let ch_server_tz = ch
        .query("SELECT serverTimeZone() FORMAT TabSeparated")?
        .trim()
        .to_string();

    // ── Ensure destination database exists ───────────────────────────────
    ch.query(&format!("CREATE DATABASE IF NOT EXISTS {}", dst.database))?;

    // ── Validate replica identity keys ──────────────────────────────────
    // Priority for identity-column selection:
    //   1. PRIMARY KEY if present
    //   2. else: smallest UNIQUE non-partial non-expression index with all
    //      columns NOT NULL
    //   3. else: bail
    // After selection, verify PG's relreplident actually carries those
    // columns in WAL OLD images (bail with a fix-it ALTER otherwise).
    info!("Validating replica identity keys...");
    let mut table_pks: Vec<(String, Vec<String>)> = Vec::new();
    for table in &config.tables {
        let rows = pg.query(&format!(
            "WITH pick AS ( \
               SELECT i.indrelid, i.indkey, i.indisprimary, \
                      i.indexrelid::regclass::text AS index_name \
               FROM pg_index i \
               WHERE i.indrelid = '{}.{}'::regclass \
                 AND (i.indisprimary OR ( \
                       i.indisunique AND i.indexprs IS NULL AND i.indpred IS NULL \
                       AND NOT EXISTS ( \
                         SELECT 1 FROM pg_attribute a \
                         WHERE a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                           AND NOT a.attnotnull))) \
               ORDER BY i.indisprimary DESC, array_length(i.indkey, 1) ASC \
               LIMIT 1 \
             ) \
             SELECT a.attname, p.indisprimary, p.index_name \
             FROM pick p \
             JOIN pg_attribute a ON a.attrelid = p.indrelid AND a.attnum = ANY(p.indkey) \
             ORDER BY array_position(p.indkey, a.attnum)",
            src.schema, table
        ))?;

        if rows.is_empty() {
            bail!(
                "Table {}.{} has no primary key and no usable unique index \
                 (a usable unique index is non-partial, non-expression, with all NOT NULL columns)",
                src.schema, table
            );
        }

        let pk_cols: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
        let is_pk = rows[0][1] == "t";
        let index_name = rows[0][2].clone();

        // Verify the chosen index's columns will actually be in WAL OLD images.
        let ri_rows = pg.query(&format!(
            "SELECT c.relreplident::text, \
                    COALESCE((SELECT i.indexrelid::regclass::text \
                              FROM pg_index i \
                              WHERE i.indrelid = c.oid AND i.indisreplident), '') \
             FROM pg_class c WHERE c.oid = '{}.{}'::regclass",
            src.schema, table
        ))?;
        let relreplident = ri_rows[0][0].as_str();
        let ri_index = ri_rows[0][1].as_str();

        let compatible = match (is_pk, relreplident) {
            // PK is in WAL for DEFAULT or FULL
            (true, "d") | (true, "f") => true,
            // Chosen unique index is in WAL if PG points at THAT index, or FULL (carries all)
            (false, "i") if ri_index == index_name => true,
            (false, "f") => true,
            _ => false,
        };

        if !compatible {
            let fix = if is_pk {
                format!("ALTER TABLE {}.{} REPLICA IDENTITY DEFAULT;", src.schema, table)
            } else {
                format!(
                    "ALTER TABLE {}.{} REPLICA IDENTITY USING INDEX {};",
                    src.schema, table, index_name.rsplit('.').next().unwrap_or(&index_name)
                )
            };
            bail!(
                "Table {}.{}: chosen identity {} ({}) won't be in WAL OLD images \
                 (relreplident='{}'). Fix on PG side: {}",
                src.schema, table,
                if is_pk { "PK" } else { "UNIQUE index" },
                index_name,
                relreplident,
                fix
            );
        }

        table_pks.push((table.clone(), pk_cols));
    }

    // ── Publication ─────────────────────────────────────────────────────
    let pub_name = config.publication_name();
    let mut readded_tables: HashSet<String> = HashSet::new();
    let pub_exists = pg.query(&format!(
        "SELECT 1 FROM pg_publication WHERE pubname = '{}'", pub_name
    ))?;
    if pub_exists.is_empty() {
        let table_list: Vec<String> = config.tables.iter()
            .map(|t| format!("{}.{}", src.schema, t))
            .collect();
        pg.execute(&format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            pub_name, table_list.join(", ")
        ))?;
        info!("Created publication '{}'", pub_name);
    } else {
        info!("Publication '{}' exists", pub_name);
        let existing = pg.query(&format!(
            "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = '{}'",
            pub_name
        ))?;
        let existing_set: std::collections::HashSet<String> =
            existing.into_iter().map(|r| r[0].clone()).collect();
        let mut missing: Vec<String> = Vec::new();
        for table in &config.tables {
            let fqn = format!("{}.{}", src.schema, table);
            if !existing_set.contains(&fqn) {
                missing.push(fqn);
                readded_tables.insert(table.clone());
            }
        }
        if !missing.is_empty() {
            pg.execute(&format!(
                "ALTER PUBLICATION {} ADD TABLE {}", pub_name, missing.join(", ")
            ))?;
            info!("Added to publication: {}", missing.join(", "));
        }
    }

    // ── Replication slot ────────────────────────────────────────────────
    let slot_name = config.slot_name();
    let slot_rows = pg.query(&format!(
        "SELECT active, restart_lsn, confirmed_flush_lsn, \
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag \
         FROM pg_replication_slots WHERE slot_name = '{}'",
        slot_name
    ))?;
    if slot_rows.is_empty() {
        pg.execute(&format!(
            "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')", slot_name
        ))?;
        info!("Created replication slot '{}'", slot_name);
    } else {
        let r = &slot_rows[0];
        info!(
            "Slot '{}': active={}, restart_lsn={}, confirmed_flush_lsn={}, lag={}",
            slot_name, r[0], r[1], r[2], r[3]
        );
    }

    // ── Snapshot LSN BEFORE initial loads ────────────────────────────────
    // This ensures CDC will replay any changes that happen during loading.
    // ReplacingMergeTree deduplication makes replaying already-loaded rows harmless.
    let target_lsn_rows = pg.query("SELECT pg_current_wal_flush_lsn()::text")?;
    let target_lsn_str = target_lsn_rows[0][0].trim().to_string();
    info!("Snapshotted target LSN: {} (before initial loads)", target_lsn_str);

    // ── Per-table diff report + actions ─────────────────────────────────
    #[derive(Clone, Copy, PartialEq)]
    enum ReloadReason {
        None,
        IncompleteLoad,
        ReAdded,
        SchemaDrift,
    }

    struct TableInfo {
        table: String,
        pk_cols: Vec<String>,
        pg_oid: u32,
        pg_rows_est: i64,
        ch_table_exists: bool,
        ch_has_rows: bool,
        reload_reason: ReloadReason,
        /// Timezone this table's timestamps are stored in — read from the
        /// column types, not the config. `None` if its columns disagree.
        timezone: Option<String>,
    }

    let mut table_infos: Vec<TableInfo> = Vec::new();
    for (table, pk_cols) in &table_pks {
        let ch_table = config.ch_table_name(table);

        // Query PG OID for this table
        let oid_rows = pg.query(&format!(
            "SELECT '{}.{}'::regclass::oid", src.schema, table
        ))?;
        let pg_oid: u32 = oid_rows[0][0].parse().unwrap_or(0);

        let ch_exists = ch.query(&format!(
            "EXISTS TABLE {} FORMAT TabSeparated", ch_table
        ))?.trim().to_string();
        let ch_table_exists = ch_exists == "1";

        // Adopt whatever timezone this table already stores, pinning it into
        // the column types where it was only implied. A table on a different
        // convention from the config is legitimate — that is how an
        // incremental migration looks midway through.
        let table_timezone = if ch_table_exists {
            resolve_table_timezone(&ch, &ch_table, &config.timezone, &ch_server_tz)?
        } else {
            // Created below with the configured timezone.
            Some(config.timezone.clone())
        };

        let pg_est_rows = pg.query(&format!(
            "SELECT reltuples::bigint FROM pg_class \
             WHERE oid = '{}.{}'::regclass",
            src.schema, table
        ))?;
        let pg_rows: i64 = pg_est_rows[0][0].parse().unwrap_or(-1);

        let ch_has_rows = if ch_table_exists {
            let check = ch.query(&format!(
                "SELECT 1 FROM {} LIMIT 1 FORMAT TabSeparated", ch_table
            ))?.trim().to_string();
            !check.is_empty()
        } else {
            false
        };

        // Detect partial loads: table has data, but max version is 0 (no CDC yet)
        // and CH row count is way below PG estimate
        let mut reload_reason = if ch_has_rows && pg_rows > 1000 {
            let max_ver = ch.query(&format!(
                "SELECT max(_pg2ch_version) FROM {} SETTINGS final = 0 FORMAT TabSeparated", ch_table
            ))?.trim().to_string();
            if max_ver == "0" {
                // No CDC has ever run — check if load was complete
                let ch_count_str = ch.query(&format!(
                    "SELECT count() FROM {} SETTINGS final = 0 FORMAT TabSeparated", ch_table
                ))?.trim().to_string();
                let ch_count: i64 = ch_count_str.parse().unwrap_or(0);
                // If CH has less than 80% of PG estimate, it's a partial load
                if ch_count < (pg_rows as f64 * 0.8) as i64 {
                    ReloadReason::IncompleteLoad
                } else {
                    ReloadReason::None
                }
            } else {
                ReloadReason::None // CDC has run, row diff is legitimate
            }
        } else {
            ReloadReason::None
        };

        // Table was dropped+recreated (re-added to publication this run)
        // and already has data in CH → needs full reload
        if ch_has_rows && readded_tables.contains(table) {
            warn!(
                "Table {} was re-added to publication (dropped+recreated externally) — scheduling reload",
                table
            );
            reload_reason = ReloadReason::ReAdded;
        }

        // Schema drift: CH and PG data-column lists no longer match.
        // Compare column NAMES in order (lightweight check, runs for every
        // existing CH table). On mismatch, schedule a reload — the existing
        // create + truncate loop below will DROP+CREATE with the current PG
        // schema and the loader will repopulate from postgresql().
        if ch_table_exists && reload_reason == ReloadReason::None {
            let (ch_db, ch_tbl) = ch_table.split_once('.').unwrap_or(("default", &ch_table));
            let ch_cols_raw = ch.query(&format!(
                "SELECT name FROM system.columns \
                 WHERE database='{}' AND table='{}' AND name NOT LIKE '_pg2ch_%' \
                 ORDER BY position FORMAT TabSeparated",
                ch_db, ch_tbl
            ))?;
            let ch_names: Vec<String> = ch_cols_raw.lines()
                .filter(|l| !l.is_empty())
                .map(|s| s.to_string())
                .collect();

            let pg_cols_raw = pg.query(&format!(
                "SELECT a.attname FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = '{}' AND c.relname = '{}' \
                   AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                src.schema, table
            ))?;
            let pg_names: Vec<String> = pg_cols_raw.iter().map(|r| r[0].clone()).collect();

            if ch_names != pg_names {
                warn!(
                    "Schema drift on {}: CH columns {:?} differ from PG {:?} — scheduling reload",
                    table, ch_names, pg_names
                );
                reload_reason = ReloadReason::SchemaDrift;
            }
        }

        table_infos.push(TableInfo {
            table: table.clone(),
            pk_cols: pk_cols.clone(),
            pg_oid,
            pg_rows_est: pg_rows,
            ch_table_exists,
            ch_has_rows,
            reload_reason,
            timezone: table_timezone,
        });
    }

    // Sort by estimated row count ascending (small tables first, big ones last)
    table_infos.sort_by_key(|t| t.pg_rows_est);

    // Print status table
    info!("─── Table status (sorted by PG rows ascending) ────────────────");
    info!("{:<30} {:>12} {:>12}  {}", "TABLE", "PG rows(est)", "CH rows", "ACTION");
    info!("{}", "─".repeat(80));

    let mut tables_to_load: Vec<&TableInfo> = Vec::new();

    for ti in &table_infos {
        let (action, ch_display) = if ti.reload_reason != ReloadReason::None {
            let reason = match ti.reload_reason {
                ReloadReason::IncompleteLoad => "RELOAD (incomplete load)",
                ReloadReason::ReAdded => "RELOAD (re-added)",
                ReloadReason::SchemaDrift => "RELOAD (schema drift)",
                ReloadReason::None => unreachable!(),
            };
            (reason, "STALE".to_string())
        } else if !ti.ch_table_exists {
            ("CREATE + LOAD", "—".to_string())
        } else if !ti.ch_has_rows && ti.pg_rows_est > 0 {
            ("LOAD", "0".to_string())
        } else if ti.ch_has_rows {
            ("CDC", "✓".to_string())
        } else {
            ("SKIP (empty)", "0".to_string())
        };

        let pg_display = if ti.pg_rows_est >= 0 {
            format!("{}", ti.pg_rows_est)
        } else {
            "?".to_string()
        };
        info!("{:<30} {:>12} {:>12}  {}", ti.table, pg_display, ch_display, action);

        if ti.reload_reason != ReloadReason::None || !ti.ch_table_exists || (!ti.ch_has_rows && ti.pg_rows_est > 0) {
            tables_to_load.push(ti);
        }
    }
    info!("{}", "─".repeat(80));

    // ── Create CH tables + truncate partial loads + heal schema drift ───
    for ti in &tables_to_load {
        let ch_table = config.ch_table_name(&ti.table);

        if !ti.ch_table_exists {
            create_ch_table(&ch, config, &ti.table, &ch_table, &ti.pk_cols)?;
            continue;
        }

        // CH table exists. Before loading, compare its current schema (data
        // cols only — ignoring _pg2ch_* meta) against PG's current schema.
        // If they differ (column added/removed/renamed/retyped on PG),
        // drop + recreate to match the latest PG schema. Safe because the
        // table is either empty (LOAD) or about to be wiped (RELOAD).
        let (ch_db, ch_tbl) = ch_table.split_once('.').unwrap_or(("default", &ch_table));
        let ch_schema_raw = ch.query(&format!(
            "SELECT name, type FROM system.columns \
             WHERE database='{}' AND table='{}' AND name NOT LIKE '_pg2ch_%' \
             ORDER BY position FORMAT TabSeparated",
            ch_db, ch_tbl
        ))?;
        let ch_schema: Vec<(String, String)> = ch_schema_raw.lines()
            .filter(|l| !l.is_empty())
            .filter_map(|line| {
                let mut p = line.split('\t');
                Some((p.next()?.to_string(), p.next()?.to_string()))
            })
            .collect();

        let pg_schema_raw = ch.query(&format!(
            "DESCRIBE TABLE postgresql('{}:{}', '{}', '{}', '{}', '{}', '{}') FORMAT TabSeparated",
            src.host, src.port, src.database, ti.table, src.user, src.password, src.schema
        ))?;
        let pg_schema: Vec<(String, String)> = pg_schema_raw.lines()
            .filter(|l| !l.is_empty())
            .filter_map(|line| {
                let mut p = line.split('\t');
                Some((p.next()?.to_string(), p.next()?.to_string()))
            })
            .collect();

        if ch_schema != pg_schema {
            warn!(
                "Schema drift on {}: dropping and recreating from current PG schema",
                ch_table
            );
            warn!("  CH had ({} cols): {:?}", ch_schema.len(), ch_schema);
            warn!("  PG now ({} cols): {:?}", pg_schema.len(), pg_schema);
            ch.query(&format!("DROP TABLE {} SYNC", ch_table))?;
            create_ch_table(&ch, config, &ti.table, &ch_table, &ti.pk_cols)?;
        } else if ti.reload_reason != ReloadReason::None {
            warn!("Truncating {} (reload scheduled)", ch_table);
            ch.query(&format!("TRUNCATE TABLE {}", ch_table))?;
        }
    }

    // ── Migrate existing tables: add _pg2ch_rel_id column if missing ──
    for ti in &table_infos {
        if ti.ch_table_exists {
            let ch_table = config.ch_table_name(&ti.table);
            let (ch_db, ch_tbl) = ch_table.split_once('.').unwrap_or(("default", &ch_table));
            let has_rel_id = ch.query(&format!(
                "SELECT count() FROM system.columns \
                 WHERE database='{}' AND table='{}' AND name='_pg2ch_rel_id' \
                 FORMAT TabSeparated",
                ch_db, ch_tbl
            ))?.trim() == "1";
            if !has_rel_id {
                ch.query(&format!(
                    "ALTER TABLE {} ADD COLUMN _pg2ch_rel_id UInt32 DEFAULT 0", ch_table
                ))?;
                info!("Added _pg2ch_rel_id column to {}", ch_table);
            }
        }
    }

    // ── Initial loads (parallel with progress monitoring) ────────────────
    if !tables_to_load.is_empty() {
        let parallel = config.settings.parallel_loads.max(1);
        info!(
            "Loading {} tables ({} parallel)...",
            tables_to_load.len(), parallel
        );

        // Queue includes pg_rows_est for progress monitoring, pg_oid for
        // _pg2ch_rel_id, and the table's own timezone for the load.
        type LoadItem = (String, String, String, String, i64, u32, Option<String>);
        let load_items: Vec<LoadItem> = tables_to_load.iter()
            .map(|ti| {
                let ch_table = config.ch_table_name(&ti.table);
                let (ch_db, ch_tbl) = ch_table.split_once('.').unwrap_or(("default", &ch_table));
                (ti.table.clone(), ch_table.clone(), ch_db.to_string(), ch_tbl.to_string(), ti.pg_rows_est, ti.pg_oid, ti.timezone.clone())
            })
            .collect();

        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let queue: Arc<Mutex<std::collections::VecDeque<LoadItem>>> =
            Arc::new(Mutex::new(load_items.into_iter().collect()));

        let mut handles = Vec::new();
        for worker_id in 0..parallel {
            let queue = Arc::clone(&queue);
            let errors = Arc::clone(&errors);
            let src_host = src.host.clone();
            let src_port = src.port;
            let src_database = src.database.clone();
            let src_user = src.user.clone();
            let src_password = src.password.clone();
            let src_schema = src.schema.clone();
            let dst_host = dst.host.clone();
            let dst_port = dst.port;
            let dst_user = dst.user.clone();
            let dst_password = dst.password.clone();
            let dst_timezone = config.timezone.clone();
            let ch_timeout = config.settings.ch_timeout_secs;

            handles.push(std::thread::spawn(move || {
                let ch = ChClient::new(&dst_host, dst_port, &dst_user, &dst_password, ch_timeout, &dst_timezone);
                let worker_pg = PgClient::connect(&src_host, src_port, &src_database, &src_user, &src_password)
                    .expect("Worker PG connect failed");
                loop {
                    let item = {
                        let mut q = queue.lock().unwrap();
                        q.pop_front()
                    };
                    let (table, ch_table, ch_db, ch_tbl, pg_rows_est, _, table_tz) = match item {
                        Some(item) => item,
                        None => break,
                    };

                    // The load resolves naive PG timestamps once for the whole
                    // INSERT, so it needs one unambiguous timezone. Only this
                    // table is skipped; the rest of the mirror is unaffected.
                    let Some(table_tz) = table_tz else {
                        let msg = format!(
                            "{}: cannot initial-load a table whose DateTime columns declare \
                             different timezones — migrate them together first",
                            table
                        );
                        error!("[W{}] {}", worker_id, msg);
                        errors.lock().unwrap().push(msg);
                        continue;
                    };

                    // Get column names
                    let col_response = match ch.query(&format!(
                        "SELECT name FROM system.columns \
                         WHERE database = '{}' AND table = '{}' \
                         AND name NOT LIKE '_pg2ch_%' ORDER BY position FORMAT TabSeparated",
                        ch_db, ch_tbl
                    )) {
                        Ok(r) => r,
                        Err(e) => {
                            error!("[W{}] Failed to get columns for {}: {:#}", worker_id, ch_table, e);
                            errors.lock().unwrap().push(format!("{}: {}", table, e));
                            continue;
                        }
                    };
                    let columns: Vec<&str> = col_response.lines().filter(|l| !l.is_empty()).collect();

                    // Query OID right before the load to minimize the race window
                    // between OID capture and postgresql() reading the table.
                    let pg_oid: u32 = match worker_pg.query(&format!(
                        "SELECT '{}.{}'::regclass::oid", src_schema, table
                    )) {
                        Ok(rows) => rows[0][0].parse().unwrap_or(0),
                        Err(e) => {
                            error!("[W{}] Failed to get OID for {}: {:#}", worker_id, table, e);
                            0
                        }
                    };

                    // session_timezone must match THIS table's timezone: unlike a
                    // TabSeparated insert (where each column parses against its own
                    // declared timezone), postgresql() resolves naive timestamps once
                    // per request, and the resulting instant is merely copied into the
                    // column. A query-level SETTINGS overrides the client-wide default.
                    let insert = format!(
                        "INSERT INTO {} ({}, _pg2ch_rel_id, _pg2ch_synced_at, _pg2ch_is_deleted, _pg2ch_version) \
                         SELECT *, {}, now64(), 0, 0 FROM postgresql('{}:{}', '{}', '{}', '{}', '{}', '{}') \
                         SETTINGS session_timezone = '{}'",
                        ch_table,
                        columns.join(", "),
                        pg_oid,
                        src_host, src_port, src_database, table, src_user, src_password, src_schema,
                        table_tz
                    );

                    info!("[W{}] Loading {}.{} → {} (~{} rows)...", worker_id, src_schema, table, ch_table, pg_rows_est);

                    // Spawn progress monitor for tables with > 1M estimated rows
                    let stop_monitor = Arc::new(AtomicBool::new(false));
                    let monitor_handle = if pg_rows_est > 1_000_000 {
                        let stop = Arc::clone(&stop_monitor);
                        let mon_ch = ChClient::new(&dst_host, dst_port, &dst_user, &dst_password, 30, &dst_timezone);
                        let mon_ch_table = ch_table.clone();
                        let mon_table = table.clone();
                        let mon_wid = worker_id;
                        let mon_est = pg_rows_est;
                        let mon_start = Instant::now();
                        Some(std::thread::spawn(move || {
                            std::thread::sleep(Duration::from_secs(60)); // first check after 1 min
                            while !stop.load(Ordering::Relaxed) {
                                if let Ok(count_str) = mon_ch.query(&format!(
                                    "SELECT count() FROM {} SETTINGS final = 0 FORMAT TabSeparated", mon_ch_table
                                )) {
                                    let ch_count: u64 = count_str.trim().parse().unwrap_or(0);
                                    if ch_count > 0 {
                                        let elapsed = mon_start.elapsed().as_secs();
                                        let rows_per_sec = ch_count as f64 / elapsed as f64;
                                        let remaining = if mon_est > 0 && ch_count < mon_est as u64 {
                                            let left = mon_est as u64 - ch_count;
                                            let eta_secs = left as f64 / rows_per_sec;
                                            if eta_secs >= 3600.0 {
                                                format!("ETA {:.1}h", eta_secs / 3600.0)
                                            } else {
                                                format!("ETA {:.0}m", eta_secs / 60.0)
                                            }
                                        } else {
                                            "finishing".to_string()
                                        };
                                        let pct = if mon_est > 0 {
                                            (ch_count as f64 / mon_est as f64 * 100.0).min(100.0)
                                        } else {
                                            0.0
                                        };
                                        info!(
                                            "[W{}] {} progress: {:.1}% ({}/{} rows, {:.0} rows/s, {})",
                                            mon_wid, mon_table, pct, ch_count, mon_est,
                                            rows_per_sec, remaining
                                        );
                                    }
                                }
                                // Sleep 60s in 1s increments to check stop flag
                                for _ in 0..60 {
                                    if stop.load(Ordering::Relaxed) { break; }
                                    std::thread::sleep(Duration::from_secs(1));
                                }
                            }
                        }))
                    } else {
                        None
                    };

                    // Initial bulk load via postgresql() can take many hours on
                    // billion-row tables. Override the configured timeout to 24h
                    // so a slow PG side doesn't kill the load and force a restart.
                    let result = ch.query_with_timeout(&insert, 86400);
                    stop_monitor.store(true, Ordering::Relaxed);
                    if let Some(h) = monitor_handle {
                        let _ = h.join();
                    }

                    if let Err(e) = result {
                        error!("[W{}] Failed to load {}: {:#}", worker_id, ch_table, e);
                        errors.lock().unwrap().push(format!("{}: {:#}", table, e));
                        continue;
                    }

                    match ch.query(&format!(
                        "SELECT count() FROM {} SETTINGS final = 0 FORMAT TabSeparated", ch_table
                    )) {
                        Ok(loaded) => info!("[W{}] Loaded {} rows into {}", worker_id, loaded.trim(), ch_table),
                        Err(_) => info!("[W{}] Loaded {} (count unavailable)", worker_id, ch_table),
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().expect("Worker thread panicked");
        }

        let errs = errors.lock().unwrap();
        if !errs.is_empty() {
            bail!("Initial load failed for {} tables:\n  {}", errs.len(), errs.join("\n  "));
        }
    }

    // ── Apply pending CDC changes ──────────────────────────────────────
    // Uses the LSN snapshotted BEFORE initial loads, so CDC replays any
    // changes made during loading. ReplacingMergeTree deduplicates harmlessly.
    info!("Applying pending WAL changes...");
    let cdc_tables: Vec<(String, String)> = config.tables.iter()
        .map(|t| (t.clone(), config.ch_table_name(t)))
        .collect();
    let cfg = CdcConfig {
        timezone: config.timezone.clone(),
        pg_host: src.host.clone(),
        pg_port: src.port,
        pg_user: src.user.clone(),
        pg_password: src.password.clone(),
        pg_database: src.database.clone(),
        ch_host: dst.host.clone(),
        ch_port: dst.port,
        ch_user: dst.user.clone(),
        ch_password: dst.password.clone(),
        slot: slot_name,
        publication: pub_name,
        tables: cdc_tables,
        batch_size: config.settings.batch_size,
        flush_interval: Duration::from_secs(config.settings.flush_interval_secs),
        binary: config.settings.binary,
        ch_timeout_secs: config.settings.ch_timeout_secs,
        target_lsn: Some(target_lsn_str),
    };
    let applied = drain_cdc(&cfg)?;
    if applied == 0 {
        info!("No pending changes — everything is in sync.");
    }

    // ── Post-CDC integrity check ────────────────────────────────────────
    // After CDC, verify tables that were loaded this run. If a load was
    // partial (crashed mid-INSERT), CDC won't fix missing rows.
    // Next run will detect via max(_pg2ch_version)=0 check and reload.
    info!("Post-CDC integrity check...");
    let mut integrity_issues = 0;
    for ti in &table_infos {
        if !ti.ch_has_rows && ti.pg_rows_est > 1000 {
            // This table was loaded this run — verify it
            let ch_table = config.ch_table_name(&ti.table);
            let ch_count_str = ch.query(&format!(
                "SELECT count() FROM {} SETTINGS final = 0 FORMAT TabSeparated", ch_table
            ))?.trim().to_string();
            let ch_count: i64 = ch_count_str.parse().unwrap_or(0);
            if ch_count < (ti.pg_rows_est as f64 * 0.8) as i64 {
                warn!(
                    "Integrity: {} has {} CH rows vs ~{} PG estimate ({:.1}%) — possible partial load, will reload next run",
                    ti.table, ch_count, ti.pg_rows_est,
                    ch_count as f64 / ti.pg_rows_est as f64 * 100.0
                );
                integrity_issues += 1;
            }
        }
    }
    if integrity_issues == 0 {
        info!("All tables passed integrity check.");
    }

    Ok(())
}

/// Check the configured timezone exists in ClickHouse and does not observe DST.
///
/// Both halves are load-bearing. An unknown timezone name makes ClickHouse
/// error out here rather than at the first insert. A DST-observing timezone
/// cannot faithfully store naive PostgreSQL `timestamp` values: the
/// spring-forward hour does not exist in it, so values inside that hour
/// collapse onto the hour before (two distinct PostgreSQL values become one
/// stored instant, irrecoverably), and the autumn fall-back hour occurs twice
/// so its instant is ambiguous.
fn validate_timezone_in_ch(ch: &ChClient, tz: &str, allow_dst: bool) -> Result<()> {
    // An unknown timezone makes ClickHouse fail this query.
    let observes_dst = ch
        .query(&format!(
            "SELECT timeZoneOffset(toDateTime('2024-01-15 12:00:00', '{tz}')) \
             != timeZoneOffset(toDateTime('2024-07-15 12:00:00', '{tz}')) \
             FORMAT TabSeparated"
        ))
        .with_context(|| {
            format!(
                "ClickHouse does not recognise timezone '{tz}' \
                 (see `SELECT * FROM system.time_zones`)"
            )
        })?;

    if observes_dst.trim() == "1" && allow_dst {
        // Deliberate, written-down choice — but never a quiet one.
        warn!("─────────────────────────────────────────────────────────────");
        warn!("timezone: {} observes DST (timezone_allow_dst: true)", tz);
        warn!("PostgreSQL `timestamp` values inside the spring-forward hour");
        warn!("cannot be represented in {} and are stored one hour early,", tz);
        warn!("indistinguishable from real values at that earlier hour.");
        warn!("Fall-back-hour instants are ambiguous. Both are unfixable in");
        warn!("place — repair needs a re-read from PostgreSQL by primary key.");
        warn!("See .claude/rules/timezones.md for the detection query and the");
        warn!("migration to a fixed-offset timezone.");
        warn!("─────────────────────────────────────────────────────────────");
        return Ok(());
    }

    if observes_dst.trim() == "1" {
        bail!(
            "timezone: {tz} observes daylight saving time, which cannot store \
             PostgreSQL `timestamp` values faithfully.\n\n\
             In {tz} the spring-forward hour does not exist. A PostgreSQL value \
             inside it has no representable instant, so ClickHouse folds it onto \
             the hour before — where real data already lives. Two different \
             source values then share one stored value and cannot be told apart \
             afterwards. The autumn fall-back hour has the mirror-image problem: \
             it occurs twice, so the stored instant is ambiguous.\n\n\
             Use a fixed-offset timezone. UTC is the right answer almost always:\n\n    \
             timezone: UTC\n\n\
             If this mirror already holds data on {tz} and you need it to keep \
             running while you plan a migration, opt in explicitly:\n\n    \
             timezone_allow_dst: true\n\n\
             That accepts the defect above for every future run and warns on each \
             one. Do not set it for a new mirror."
        );
    }

    info!("Timezone: {} (no DST — timestamps round-trip exactly)", tz);
    Ok(())
}

/// Resolve which timezone an existing ClickHouse table stores timestamps in,
/// and make that choice explicit in the column types.
///
/// The column type is the authority, not the config. A column that declares a
/// timezone parses and renders against that timezone no matter what the
/// session or the config says, so a table already migrated to another
/// convention is not a problem to be refused — it is a fact to be adopted.
/// That is what lets a mirror be migrated one table at a time.
///
/// Returns the table's timezone, or `None` if its DateTime columns disagree
/// with each other (which only obstructs an initial load — see the loader).
fn resolve_table_timezone(
    ch: &ChClient,
    ch_table: &str,
    config_tz: &str,
    server_tz: &str,
) -> Result<Option<String>> {
    let (ch_db, ch_tbl) = ch_table.split_once('.').unwrap_or(("default", ch_table));
    // TabSeparatedRaw: TabSeparated escapes the quotes inside the type string.
    let rows = ch.query(&format!(
        "SELECT name, type FROM system.columns \
         WHERE database='{}' AND table='{}' ORDER BY position FORMAT TabSeparatedRaw",
        ch_db, ch_tbl
    ))?;

    let mut declared: Vec<(String, String)> = Vec::new();
    let mut bare: Vec<(String, String)> = Vec::new();
    for line in rows.lines().filter(|l| !l.trim().is_empty()) {
        let Some((name, ch_type)) = line.split_once('\t') else { continue };
        if !has_datetime(ch_type) {
            continue;
        }
        match datetime_timezone(ch_type) {
            Some(tz) => declared.push((name.to_string(), tz)),
            None => bare.push((name.to_string(), ch_type.to_string())),
        }
    }

    if declared.is_empty() && bare.is_empty() {
        // No DateTime columns; the value is irrelevant for this table.
        return Ok(Some(config_tz.to_string()));
    }

    // A table whose columns disagree cannot be initial-loaded correctly: the
    // load resolves naive values once per INSERT, so only the columns matching
    // that one timezone come out right. CDC is unaffected (it parses per
    // column), so warn rather than stop.
    let distinct: HashSet<&str> = declared.iter().map(|(_, tz)| tz.as_str()).collect();
    if distinct.len() > 1 {
        warn!(
            "{} mixes timezones across its DateTime columns ({}). CDC handles this \
             correctly — each column parses against its own timezone — but an initial \
             load of this table cannot. Migrate all of a table's DateTime columns together.",
            ch_table,
            declared.iter()
                .map(|(n, tz)| format!("{}={}", n, tz))
                .collect::<Vec<_>>().join(", ")
        );
        return Ok(None);
    }

    // The table's convention: what its columns already declare, else — for a
    // table created before timezones were pinned — whatever the ClickHouse
    // server default was when it was written.
    let table_tz = match distinct.into_iter().next() {
        Some(tz) => tz.to_string(),
        None => {
            if server_tz != config_tz {
                warn!(
                    "{} predates timezone pinning, so its timestamps are stored on the \
                     ClickHouse server default '{}', not the configured '{}'. Pinning it to \
                     '{}' to record what the data actually is. To move it to '{}', rewrite it \
                     first — see .claude/rules/timezones.md — and this run will then adopt it.",
                    ch_table, server_tz, config_tz, server_tz, config_tz
                );
            }
            server_tz.to_string()
        }
    };

    if !bare.is_empty() {
        // Metadata-only: ClickHouse rewrites no data for a timezone-only
        // change, so this is safe on a table of any size.
        let clauses: Vec<String> = bare
            .iter()
            .map(|(name, ty)| {
                format!("MODIFY COLUMN `{}` {}", name, pin_datetime_timezone(ty, &table_tz))
            })
            .collect();
        ch.query(&format!("ALTER TABLE {} {}", ch_table, clauses.join(", ")))?;
        info!(
            "  Pinned timezone '{}' on {} column(s) of {} (metadata-only)",
            table_tz, bare.len(), ch_table
        );
    }

    // Reported whether or not anything was pinned above: a disagreement between
    // the table and the config is either a migration in progress or a mistake,
    // and the two are indistinguishable from here. Warn rather than inform so
    // the mistake is visible — a wrong `timezone:` no longer stops the run, and
    // silence would let it create new tables on a second convention unnoticed.
    if table_tz != config_tz {
        warn!(
            "{} stores timestamps in '{}' but the config says timezone: {} — \
             using the table's own '{}'. The column type is the authority, so this \
             is expected part-way through a per-table migration. If it is not \
             deliberate then the config is wrong: any NEW table will be created in \
             '{}', leaving the mirror on two conventions.",
            ch_table, table_tz, config_tz, table_tz, config_tz
        );
    }

    Ok(Some(table_tz))
}

/// Create CH table from PG schema using DESCRIBE TABLE postgresql().
fn create_ch_table(
    ch: &ChClient,
    config: &MirrorConfig,
    table: &str,
    ch_table: &str,
    pk_cols: &[String],
) -> Result<()> {
    let src = &config.source;

    let describe = ch.query(&format!(
        "DESCRIBE TABLE postgresql('{}:{}', '{}', '{}', '{}', '{}', '{}') FORMAT TabSeparated",
        src.host, src.port, src.database, table, src.user, src.password, src.schema
    ))?;

    let tz = &config.timezone;
    let mut col_defs: Vec<String> = Vec::new();
    for line in describe.lines() {
        if line.trim().is_empty() { continue; }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 2 {
            // ClickHouse chose the type; we only make the timezone of any
            // DateTime in it explicit. DESCRIBE always omits it, which would
            // leave the column silently bound to the server default.
            col_defs.push(format!("    {} {}", parts[0], pin_datetime_timezone(parts[1], tz)));
        }
    }
    if col_defs.is_empty() {
        bail!("DESCRIBE returned no columns for {}.{}", src.schema, table);
    }

    col_defs.push("    _pg2ch_rel_id UInt32 DEFAULT 0".to_string());
    col_defs.push(format!(
        "    _pg2ch_synced_at DateTime64(9, '{}') DEFAULT now64()", tz
    ));
    col_defs.push("    _pg2ch_is_deleted UInt8 DEFAULT 0".to_string());
    col_defs.push("    _pg2ch_version UInt64 DEFAULT 0".to_string());

    let ddl = format!(
        "CREATE TABLE {} (\n{}\n) ENGINE = ReplacingMergeTree(_pg2ch_version, _pg2ch_is_deleted)\nORDER BY ({})",
        ch_table,
        col_defs.join(",\n"),
        pk_cols.join(", ")
    );

    ch.query(&ddl)?;
    info!("  Created {} ({} cols, PK: ({}))", ch_table, col_defs.len() - 4, pk_cols.join(", "));
    Ok(())
}
