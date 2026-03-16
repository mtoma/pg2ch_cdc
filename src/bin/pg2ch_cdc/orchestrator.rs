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

use anyhow::{bail, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn, error};

use crate::cdc::{CdcConfig, drain_cdc};
use pg2ch_cdc::clickhouse::ChClient;
use pg2ch_cdc::config::MirrorConfig;
use pg2ch_cdc::pg::PgClient;

pub fn run_mirror(config: &MirrorConfig) -> Result<()> {
    let src = &config.source;
    let dst = &config.destination;

    // ── Connect ─────────────────────────────────────────────────────────
    let pg = PgClient::connect(&src.host, src.port, &src.database, &src.user, &src.password)?;
    let ch = ChClient::new(&dst.host, dst.port, &dst.user, &dst.password, config.settings.ch_timeout_secs);

    // ── Validate primary keys ───────────────────────────────────────────
    info!("Validating primary keys...");
    let mut table_pks: Vec<(String, Vec<String>)> = Vec::new();
    for table in &config.tables {
        let rows = pg.query(&format!(
            "SELECT a.attname \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary \
             ORDER BY array_position(i.indkey, a.attnum)",
            src.schema, table
        ))?;
        if rows.is_empty() {
            bail!("Table {}.{} has no primary key", src.schema, table);
        }
        let pk_cols: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
        table_pks.push((table.clone(), pk_cols));
    }

    // ── Publication ─────────────────────────────────────────────────────
    let pub_name = config.publication_name();
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
    struct TableInfo {
        table: String,
        pk_cols: Vec<String>,
        pg_rows_est: i64,
        ch_table_exists: bool,
        ch_has_rows: bool,
        needs_reload: bool, // partial load detected
    }

    let mut table_infos: Vec<TableInfo> = Vec::new();
    for (table, pk_cols) in &table_pks {
        let ch_table = config.ch_table_name(table);

        let ch_exists = ch.query(&format!(
            "EXISTS TABLE {} FORMAT TabSeparated", ch_table
        ))?.trim().to_string();
        let ch_table_exists = ch_exists == "1";

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
        let needs_reload = if ch_has_rows && pg_rows > 1000 {
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
                ch_count < (pg_rows as f64 * 0.8) as i64
            } else {
                false // CDC has run, row diff is legitimate
            }
        } else {
            false
        };

        table_infos.push(TableInfo {
            table: table.clone(),
            pk_cols: pk_cols.clone(),
            pg_rows_est: pg_rows,
            ch_table_exists,
            ch_has_rows,
            needs_reload,
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
        let (action, ch_display) = if ti.needs_reload {
            ("RELOAD (partial)", "PARTIAL".to_string())
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

        if ti.needs_reload || !ti.ch_table_exists || (!ti.ch_has_rows && ti.pg_rows_est > 0) {
            tables_to_load.push(ti);
        }
    }
    info!("{}", "─".repeat(80));

    // ── Create CH tables + truncate partial loads ────────────────────────
    for ti in &tables_to_load {
        let ch_table = config.ch_table_name(&ti.table);
        if !ti.ch_table_exists {
            create_ch_table(&ch, config, &ti.table, &ch_table, &ti.pk_cols)?;
        }
        if ti.needs_reload {
            warn!("Truncating {} (partial load detected)", ch_table);
            ch.query(&format!("TRUNCATE TABLE {}", ch_table))?;
        }
    }

    // ── Initial loads (parallel with progress monitoring) ────────────────
    if !tables_to_load.is_empty() {
        let parallel = config.settings.parallel_loads.max(1);
        info!(
            "Loading {} tables ({} parallel)...",
            tables_to_load.len(), parallel
        );

        // Queue includes pg_rows_est for progress monitoring
        let load_items: Vec<(String, String, String, String, i64)> = tables_to_load.iter()
            .map(|ti| {
                let ch_table = config.ch_table_name(&ti.table);
                let (ch_db, ch_tbl) = ch_table.split_once('.').unwrap_or(("default", &ch_table));
                (ti.table.clone(), ch_table.clone(), ch_db.to_string(), ch_tbl.to_string(), ti.pg_rows_est)
            })
            .collect();

        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let queue: Arc<Mutex<std::collections::VecDeque<(String, String, String, String, i64)>>> =
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
            let ch_timeout = config.settings.ch_timeout_secs;

            handles.push(std::thread::spawn(move || {
                let ch = ChClient::new(&dst_host, dst_port, &dst_user, &dst_password, ch_timeout);
                loop {
                    let item = {
                        let mut q = queue.lock().unwrap();
                        q.pop_front()
                    };
                    let (table, ch_table, ch_db, ch_tbl, pg_rows_est) = match item {
                        Some(item) => item,
                        None => break,
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

                    let insert = format!(
                        "INSERT INTO {} ({}, _pg2ch_synced_at, _pg2ch_is_deleted, _pg2ch_version) \
                         SELECT *, now64(), 0, 0 FROM postgresql('{}:{}', '{}', '{}', '{}', '{}', '{}')",
                        ch_table,
                        columns.join(", "),
                        src_host, src_port, src_database, table, src_user, src_password, src_schema
                    );

                    info!("[W{}] Loading {}.{} → {} (~{} rows)...", worker_id, src_schema, table, ch_table, pg_rows_est);

                    // Spawn progress monitor for tables with > 1M estimated rows
                    let stop_monitor = Arc::new(AtomicBool::new(false));
                    let monitor_handle = if pg_rows_est > 1_000_000 {
                        let stop = Arc::clone(&stop_monitor);
                        let mon_ch = ChClient::new(&dst_host, dst_port, &dst_user, &dst_password, 30);
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

                    let result = ch.query(&insert);
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

    let mut col_defs: Vec<String> = Vec::new();
    for line in describe.lines() {
        if line.trim().is_empty() { continue; }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 2 {
            col_defs.push(format!("    {} {}", parts[0], parts[1]));
        }
    }
    if col_defs.is_empty() {
        bail!("DESCRIBE returned no columns for {}.{}", src.schema, table);
    }

    col_defs.push("    _pg2ch_synced_at DateTime64(9) DEFAULT now64()".to_string());
    col_defs.push("    _pg2ch_is_deleted UInt8 DEFAULT 0".to_string());
    col_defs.push("    _pg2ch_version UInt64 DEFAULT 0".to_string());

    let ddl = format!(
        "CREATE TABLE {} (\n{}\n) ENGINE = ReplacingMergeTree(_pg2ch_version, _pg2ch_is_deleted)\nORDER BY ({})",
        ch_table,
        col_defs.join(",\n"),
        pk_cols.join(", ")
    );

    ch.query(&ddl)?;
    info!("  Created {} ({} cols, PK: ({}))", ch_table, col_defs.len() - 3, pk_cols.join(", "));
    Ok(())
}
