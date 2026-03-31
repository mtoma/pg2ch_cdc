//! Diff engine — progressive validation levels for PG ↔ CH comparison.
//!
//! Levels 1-2 use lightweight count-based checks.
//! Levels 3-4 snapshot PG into a temp CH table via postgresql(), then compare
//! entirely in CH using FULL JOIN + sipHash64. This avoids the PG bottleneck
//! for hash computation and gives point-in-time consistency.

use anyhow::{bail, Result};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use pg2ch_cdc::clickhouse::ChClient;
use pg2ch_cdc::pg::PgClient;

use crate::col_types::{self, Column};
use crate::config::{DiffConfig, DiffLevel, TableDiff};

#[derive(Debug)]
pub struct TableResult {
    pub table: String,
    pub level: DiffLevel,
    pub status: DiffStatus,
}

#[derive(Debug)]
pub enum DiffStatus {
    Match { detail: String },
    Mismatch { detail: String },
    Error { detail: String },
}

pub fn run_diff(config: &DiffConfig, skip_snapshot: bool) -> Result<Vec<TableResult>> {
    let src = &config.source;
    let dst = &config.destination;

    let pg = PgClient::connect(&src.host, src.port, &src.database, &src.user, &src.password)?;
    let ch = ChClient::new(&dst.host, dst.port, &dst.user, &dst.password, config.ch_timeout_secs);

    let mut results: Vec<TableResult> = Vec::new();

    for table_diff in &config.tables {
        info!("─── {} (level: {:?}) ───", table_diff.name, table_diff.level);

        let result = match diff_table(&pg, &ch, config, table_diff, skip_snapshot) {
            Ok(r) => r,
            Err(e) => TableResult {
                table: table_diff.name.clone(),
                level: table_diff.level.clone(),
                status: DiffStatus::Error { detail: format!("{:#}", e) },
            },
        };

        match &result.status {
            DiffStatus::Match { detail } => info!("  OK: {}", detail),
            DiffStatus::Mismatch { detail } => warn!("  MISMATCH: {}", detail),
            DiffStatus::Error { detail } => warn!("  ERROR: {}", detail),
        }

        results.push(result);
    }

    Ok(results)
}

fn format_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

fn format_number(n: i64) -> String {
    if n.abs() >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n.abs() >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n.abs() >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

fn diff_table(
    pg: &PgClient,
    ch: &ChClient,
    config: &DiffConfig,
    table_diff: &TableDiff,
    skip_snapshot: bool,
) -> Result<TableResult> {
    let table = &table_diff.name;
    let schema = &config.source.schema;
    let ch_table = config.ch_table_name(table);

    // Verify CH table exists
    let ch_exists = ch.query(&format!(
        "EXISTS TABLE {} FORMAT TabSeparated", ch_table
    ))?.trim().to_string();
    if ch_exists != "1" {
        return Ok(TableResult {
            table: table.clone(),
            level: table_diff.level.clone(),
            status: DiffStatus::Error { detail: format!("CH table {} does not exist", ch_table) },
        });
    }

    // ── Level 1: Metadata count ─────────────────────────────────────────
    let pg_est = pg.query(&format!(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = '{}.{}'::regclass",
        schema, table
    ))?;
    let pg_est_count: i64 = pg_est[0][0].parse().unwrap_or(-1);

    let ch_est = ch.query(&format!(
        "SELECT count() FROM {} SETTINGS final = 0 FORMAT TabSeparated", ch_table
    ))?.trim().to_string();
    let ch_est_count: i64 = ch_est.parse().unwrap_or(-1);

    info!("  metadata count: PG ~{} / CH ~{}", format_number(pg_est_count), format_number(ch_est_count));

    if table_diff.level == DiffLevel::MetadataCount {
        let ratio = if pg_est_count > 0 {
            ch_est_count as f64 / pg_est_count as f64
        } else {
            1.0
        };
        return if (0.8..=1.2).contains(&ratio) || (pg_est_count == 0 && ch_est_count == 0) {
            Ok(TableResult {
                table: table.clone(),
                level: DiffLevel::MetadataCount,
                status: DiffStatus::Match {
                    detail: format!("PG ~{} / CH ~{} (ratio {:.2})", format_number(pg_est_count), format_number(ch_est_count), ratio),
                },
            })
        } else {
            Ok(TableResult {
                table: table.clone(),
                level: DiffLevel::MetadataCount,
                status: DiffStatus::Mismatch {
                    detail: format!("PG ~{} / CH ~{} (ratio {:.2})", format_number(pg_est_count), format_number(ch_est_count), ratio),
                },
            })
        };
    }

    // ── Level 2: Exact count ────────────────────────────────────────────
    // Skip expensive PG count for checksum level with --skip-snapshot
    // (the snapshot count replaces it)
    let skip_exact_count = skip_snapshot
        && (table_diff.level == DiffLevel::Checksum || table_diff.level == DiffLevel::PrimaryKeys);

    let pg_count: i64;
    let ch_count: i64;

    if skip_exact_count {
        pg_count = pg_est_count; // use estimate, snapshot count will be exact
        ch_count = pg_est_count; // placeholder, will be checked later
        info!("  skipping exact counts (--skip-snapshot)");
    } else {
        info!("  counting PG rows (exact)...");
        let t0 = Instant::now();
        let pg_exact = pg.query(&format!(
            "SELECT count(*) FROM {}.{}", schema, table
        ))?;
        pg_count = pg_exact[0][0].parse().unwrap_or(-1);
        info!("  PG count: {} ({})", format_number(pg_count), format_duration(t0.elapsed().as_secs()));

        info!("  counting CH rows (FINAL)...");
        let t0 = Instant::now();
        let ch_exact = ch.query(&format!(
            "SELECT count() FROM {} FINAL FORMAT TabSeparated", ch_table
        ))?.trim().to_string();
        ch_count = ch_exact.parse().unwrap_or(-1);
        info!("  CH count: {} ({})", format_number(ch_count), format_duration(t0.elapsed().as_secs()));
    }

    if table_diff.level == DiffLevel::ExactCount {
        return if pg_count == ch_count {
            Ok(TableResult {
                table: table.clone(),
                level: DiffLevel::ExactCount,
                status: DiffStatus::Match {
                    detail: format!("both have {} rows", format_number(pg_count)),
                },
            })
        } else {
            Ok(TableResult {
                table: table.clone(),
                level: DiffLevel::ExactCount,
                status: DiffStatus::Mismatch {
                    detail: format!("PG {} / CH {} (diff {})", format_number(pg_count), format_number(ch_count), pg_count - ch_count),
                },
            })
        };
    }

    // ── Levels 3 & 4: Snapshot + CH-vs-CH comparison ─────────────────────
    // 1. Discover columns from PG
    // 2. Record CDC max version
    // 3. Snapshot PG into temp CH table via postgresql()
    // 4. Check version didn't change (table wasn't touched during snapshot)
    // 5. Compare snapshot vs CDC in CH (FULL JOIN + sipHash64)
    // 6. Drill down mismatches with rounding fallback
    // 7. Drop temp table

    let (all_columns, pk_names) = col_types::build_all_columns(pg, schema, table, config.decimal_tolerance)?;

    let hash_columns: Vec<&Column> = match table_diff.level {
        DiffLevel::PrimaryKeys => all_columns.iter().filter(|c| c.is_pk).collect(),
        DiffLevel::Checksum => all_columns.iter().collect(),
        _ => unreachable!(),
    };

    let type_summary = summarize_types(&hash_columns);
    info!("  hashing {} columns: {}", hash_columns.len(), type_summary);

    // Build CH-side hash expressions (used on both snapshot and CDC tables)
    let ch_hash_expr = ch_siphash_expr(&hash_columns, false);
    let ch_hash_expr_rounded = ch_siphash_expr(&hash_columns, true);

    // PK join condition
    let pk_join = pk_names.iter()
        .map(|pk| format!("s.{pk} = c.{pk}"))
        .collect::<Vec<_>>().join(" AND ");
    let pk_select_s = pk_names.iter()
        .map(|pk| format!("s.{pk}"))
        .collect::<Vec<_>>().join(", ");
    let pk_tostring = pk_names.iter()
        .map(|pk| format!("toString(s.{})", pk))
        .collect::<Vec<_>>().join(", '|', ");

    // Temp table
    let snapshot_table = config.snapshot_table_name(table);
    let src = &config.source;
    let pg_func = format!(
        "postgresql('{}:{}', '{}', '{}', '{}', '{}', '{}')",
        src.host, src.port, src.database, table, src.user, src.password, src.schema
    );
    let pk_order = pk_names.join(", ");

    let overall_start = Instant::now();

    // Record max version before snapshot
    let version_before = ch.query(&format!(
        "SELECT max(_pg2ch_version) FROM {} SETTINGS final = 0 FORMAT TabSeparated",
        ch_table
    ))?.trim().to_string();
    info!("  CDC version before snapshot: {}", version_before);

    let snap_count;
    if skip_snapshot {
        // Reuse existing snapshot table
        let exists = ch.query(&format!(
            "EXISTS TABLE {} FORMAT TabSeparated", snapshot_table
        ))?.trim().to_string();
        if exists != "1" {
            bail!("--skip-snapshot: table {} does not exist", snapshot_table);
        }
        snap_count = ch.query(&format!(
            "SELECT count() FROM {} FORMAT TabSeparated", snapshot_table
        ))?.trim().to_string().parse::<i64>().unwrap_or(-1);
        info!("  reusing existing snapshot: {} rows", format_number(snap_count));
    } else {
        // Create snapshot with progress monitoring
        info!("  snapshotting PG via postgresql()...");
        let t0 = Instant::now();

        // Drop leftover from previous run
        ch.query(&format!("DROP TABLE IF EXISTS {} SYNC", snapshot_table))?;

        // Create empty table with same structure as CDC table (minus _pg2ch_* columns)
        let data_columns = ch.query(&format!(
            "SELECT name, type FROM system.columns \
             WHERE database = '{}' AND table = '{}' AND name NOT LIKE '_pg2ch%' \
             ORDER BY position FORMAT TabSeparated",
            config.destination.database, table
        ))?.trim().to_string();
        let col_defs: Vec<String> = data_columns.lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                let parts: Vec<&str> = l.split('\t').collect();
                format!("{} {}", parts[0], parts.get(1).unwrap_or(&"String"))
            })
            .collect();
        ch.query(&format!(
            "CREATE TABLE {} ({}) ENGINE = MergeTree() ORDER BY ({})",
            snapshot_table, col_defs.join(", "), pk_order
        ))?;

        // Spawn progress monitor thread
        let snap_table_clone = snapshot_table.clone();
        let dst = &config.destination;
        let monitor_ch = ChClient::new(&dst.host, dst.port, &dst.user, &dst.password, 60);
        let pg_est_for_monitor = pg_est_count;
        let stop_monitor = Arc::new(AtomicBool::new(false));
        let stop_flag = stop_monitor.clone();
        let monitor_start = Instant::now();

        let monitor = std::thread::spawn(move || {
            let mut last_count: i64 = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(60));
                if stop_flag.load(Ordering::Relaxed) { break; }
                if let Ok(result) = monitor_ch.query(&format!(
                    "SELECT count() FROM {} FORMAT TabSeparated", snap_table_clone
                )) {
                    let count: i64 = result.trim().parse().unwrap_or(0);
                    let elapsed = monitor_start.elapsed().as_secs();
                    let rate = if elapsed > 0 { count as f64 / elapsed as f64 } else { 0.0 };
                    let eta = if rate > 0.0 && pg_est_for_monitor > 0 {
                        ((pg_est_for_monitor as f64 - count as f64) / rate) as u64
                    } else { 0 };
                    let delta = count - last_count;
                    info!(
                        "  [snapshot] {} rows ({:.0} rows/s, +{} since last, ETA {})",
                        format_number(count), rate, format_number(delta), format_duration(eta)
                    );
                    last_count = count;
                }
            }
        });

        // Run the actual INSERT
        ch.query(&format!(
            "INSERT INTO {} SELECT * FROM {}",
            snapshot_table, pg_func
        ))?;

        stop_monitor.store(true, Ordering::Relaxed);
        let _ = monitor.join();

        let snap_secs = t0.elapsed().as_secs();
        snap_count = ch.query(&format!(
            "SELECT count() FROM {} FORMAT TabSeparated", snapshot_table
        ))?.trim().to_string().parse::<i64>().unwrap_or(-1);
        let snap_rate = if snap_secs > 0 { snap_count as f64 / snap_secs as f64 } else { 0.0 };
        info!(
            "  snapshot: {} rows ({}, {:.0} rows/s)",
            format_number(snap_count), format_duration(snap_secs), snap_rate
        );
    }

    // Count CDC side
    let cdc_count = ch.query(&format!(
        "SELECT count() FROM {} FINAL FORMAT TabSeparated", ch_table
    ))?.trim().to_string().parse::<i64>().unwrap_or(-1);

    if snap_count != cdc_count {
        let detail = format!(
            "count mismatch: snapshot {} / CDC {} (diff {})",
            format_number(snap_count), format_number(cdc_count), snap_count - cdc_count
        );
        ch.query(&format!("DROP TABLE IF EXISTS {} SYNC", snapshot_table))?;
        return Ok(TableResult {
            table: table.clone(),
            level: table_diff.level.clone(),
            status: DiffStatus::Mismatch { detail },
        });
    }
    info!("  counts match: {} rows", format_number(snap_count));

    // ── CH-vs-CH comparison ──────────────────────────────────────────────
    info!("  comparing hashes...");
    let t0 = Instant::now();
    let compare_result = ch.query(&format!(
        "SELECT \
           countIf(s.h IS NOT NULL AND c.h IS NULL) as missing_in_cdc, \
           countIf(s.h IS NULL AND c.h IS NOT NULL) as missing_in_snapshot, \
           countIf(s.h != c.h) as hash_mismatch, \
           countIf(s.h = c.h) as matching \
         FROM (\
           SELECT {pk_select}, {hash} as h FROM {snap}\
         ) s \
         FULL JOIN (\
           SELECT {pk_select_bare}, {hash} as h FROM {cdc} FINAL\
         ) c ON {join} \
         FORMAT TabSeparated",
        pk_select = pk_names.iter().map(|pk| format!("{}", pk)).collect::<Vec<_>>().join(", "),
        pk_select_bare = pk_names.iter().map(|pk| format!("{}", pk)).collect::<Vec<_>>().join(", "),
        hash = ch_hash_expr,
        snap = snapshot_table,
        cdc = ch_table,
        join = pk_join,
    ))?.trim().to_string();
    let compare_secs = t0.elapsed().as_secs();

    let parts: Vec<&str> = compare_result.split('\t').collect();
    let missing_in_cdc: i64 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
    let missing_in_snapshot: i64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let hash_mismatch: i64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let matching: i64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);

    info!(
        "  comparison done ({}): {} matching, {} mismatches, {} missing in CDC, {} missing in snapshot",
        format_duration(compare_secs), format_number(matching),
        format_number(hash_mismatch), format_number(missing_in_cdc), format_number(missing_in_snapshot)
    );

    let total_issues = missing_in_cdc + missing_in_snapshot + hash_mismatch;

    // Drill down hash mismatches with rounding fallback
    let (ulp_noise, real_mismatch) = if hash_mismatch > 0 {
        info!("  re-checking {} hash mismatches with rounding fallback...", hash_mismatch);
        let t0 = Instant::now();

        let drilldown_result = ch.query(&format!(
            "SELECT \
               countIf(s.hr = c.hr) as ulp_noise, \
               countIf(s.hr != c.hr) as real_mismatch \
             FROM (\
               SELECT {pk_select}, {hash} as h, {hash_r} as hr FROM {snap}\
             ) s \
             JOIN (\
               SELECT {pk_select_bare}, {hash} as h, {hash_r} as hr FROM {cdc} FINAL\
             ) c ON {join} \
             WHERE s.h != c.h \
             FORMAT TabSeparated",
            pk_select = pk_names.join(", "),
            pk_select_bare = pk_names.join(", "),
            hash = ch_hash_expr,
            hash_r = ch_hash_expr_rounded,
            snap = snapshot_table,
            cdc = ch_table,
            join = pk_join,
        ))?.trim().to_string();

        let parts: Vec<&str> = drilldown_result.split('\t').collect();
        let ulp_noise: i64 = parts.get(0).and_then(|s| s.parse().ok()).unwrap_or(0);
        let real_mismatch: i64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        info!(
            "  rounding fallback ({}): {} ULP noise, {} real mismatches",
            format_duration(t0.elapsed().as_secs()), ulp_noise, real_mismatch
        );

        // Show sample real mismatches
        if real_mismatch > 0 {
            let samples = ch.query(&format!(
                "SELECT concat({pk_str}) as pk \
                 FROM (\
                   SELECT {pk_select}, {hash} as h, {hash_r} as hr FROM {snap}\
                 ) s \
                 JOIN (\
                   SELECT {pk_select_bare}, {hash} as h, {hash_r} as hr FROM {cdc} FINAL\
                 ) c ON {join} \
                 WHERE s.h != c.h AND s.hr != c.hr \
                 LIMIT 20 \
                 FORMAT TabSeparated",
                pk_str = pk_tostring,
                pk_select = pk_names.join(", "),
                pk_select_bare = pk_names.join(", "),
                hash = ch_hash_expr,
                hash_r = ch_hash_expr_rounded,
                snap = snapshot_table,
                cdc = ch_table,
                join = pk_join,
            ))?.trim().to_string();

            for line in samples.lines().filter(|l| !l.is_empty()) {
                info!("    PK({}) value mismatch", line);
            }
        }

        (ulp_noise, real_mismatch)
    } else {
        (0, 0)
    };

    // ── Version stability check (AFTER all comparison queries) ───────────
    // The CDC table must not have been modified during the entire process
    // (snapshot + comparison), otherwise results are meaningless.
    let version_after = ch.query(&format!(
        "SELECT max(_pg2ch_version) FROM {} SETTINGS final = 0 FORMAT TabSeparated",
        ch_table
    ))?.trim().to_string();

    ch.query(&format!("DROP TABLE IF EXISTS {} SYNC", snapshot_table))?;

    if version_before != version_after {
        return Ok(TableResult {
            table: table.clone(),
            level: table_diff.level.clone(),
            status: DiffStatus::Error {
                detail: format!(
                    "CDC version changed during comparison ({} → {}) — results invalid, retry later",
                    version_before, version_after
                ),
            },
        });
    }
    info!("  CDC version stable (no changes during snapshot + comparison)");

    // ── Build result ─────────────────────────────────────────────────────
    let effective_mismatches = real_mismatch + missing_in_cdc + missing_in_snapshot;

    if total_issues == 0 || effective_mismatches == 0 {
        Ok(TableResult {
            table: table.clone(),
            level: table_diff.level.clone(),
            status: DiffStatus::Match {
                detail: if ulp_noise > 0 {
                    format!(
                        "all {} rows match ({} ULP noise resolved, {})",
                        format_number(matching + ulp_noise),
                        ulp_noise,
                        format_duration(overall_start.elapsed().as_secs())
                    )
                } else {
                    format!(
                        "all {} rows match ({})",
                        format_number(matching),
                        format_duration(overall_start.elapsed().as_secs())
                    )
                },
            },
        })
    } else {
        Ok(TableResult {
            table: table.clone(),
            level: table_diff.level.clone(),
            status: DiffStatus::Mismatch {
                detail: format!(
                    "{} real mismatches, {} missing in CDC, {} missing in snapshot, {} ULP noise ({} rows matched, {})",
                    real_mismatch, missing_in_cdc, missing_in_snapshot, ulp_noise,
                    format_number(matching),
                    format_duration(overall_start.elapsed().as_secs())
                ),
            },
        })
    }
}

/// Build a sipHash64 expression for CH using either bit-masked or rounded float expressions.
/// Uses arrayStringConcat to keep AST depth flat regardless of column count.
fn ch_siphash_expr(columns: &[&Column], use_rounded: bool) -> String {
    let parts: Vec<String> = columns.iter().map(|c| {
        if use_rounded {
            c.ch_expr_rounded.clone()
        } else {
            c.ch_expr.clone()
        }
    }).collect();

    let array_elems = parts.join(", ");
    format!("sipHash64(arrayStringConcat([{}], '|'))", array_elems)
}

fn summarize_types(columns: &[&Column]) -> String {
    let mut type_counts: BTreeMap<&str, usize> = BTreeMap::new();
    for col in columns {
        *type_counts.entry(&col.pg_type).or_insert(0) += 1;
    }
    let parts: Vec<String> = type_counts
        .iter()
        .map(|(t, c)| format!("{} {}", c, t))
        .collect();
    parts.join(", ")
}
