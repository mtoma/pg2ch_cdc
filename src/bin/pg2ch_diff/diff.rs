//! Diff engine — progressive validation levels for PG ↔ CH comparison.
//!
//! Level 3 (PK comparison) uses grouped hash comparison: for each distinct
//! value of the leading PK column, compute count + hash aggregate of all PKs.
//! Groups where both count and hash match are identical. Mismatching groups
//! are reported with details.

use anyhow::{bail, Result};
use std::time::Instant;
use tracing::{info, warn};

use pg2ch_cdc::clickhouse::ChClient;
use pg2ch_cdc::pg::PgClient;

use crate::config::{DiffConfig, DiffLevel, TableDiff};
use crate::pk_types;

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

pub fn run_diff(config: &DiffConfig) -> Result<Vec<TableResult>> {
    let src = &config.source;
    let dst = &config.destination;

    let pg = PgClient::connect(&src.host, src.port, &src.database, &src.user, &src.password)?;
    let ch = ChClient::new(&dst.host, dst.port, &dst.user, &dst.password, 3600);

    let mut results: Vec<TableResult> = Vec::new();

    for table_diff in &config.tables {
        info!("─── {} (level: {:?}) ───", table_diff.name, table_diff.level);

        let result = match diff_table(&pg, &ch, config, table_diff) {
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
    info!("  counting PG rows (exact)...");
    let t0 = Instant::now();
    let pg_exact = pg.query(&format!(
        "SELECT count(*) FROM {}.{}", schema, table
    ))?;
    let pg_count: i64 = pg_exact[0][0].parse().unwrap_or(-1);
    info!("  PG count: {} ({})", format_number(pg_count), format_duration(t0.elapsed().as_secs()));

    info!("  counting CH rows (FINAL)...");
    let t0 = Instant::now();
    let ch_exact = ch.query(&format!(
        "SELECT count() FROM {} FINAL WHERE _pg2ch_is_deleted = 0 FORMAT TabSeparated", ch_table
    ))?.trim().to_string();
    let ch_count: i64 = ch_exact.parse().unwrap_or(-1);
    info!("  CH count: {} ({})", format_number(ch_count), format_duration(t0.elapsed().as_secs()));

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

    // ── Level 3: Primary key hash comparison (grouped) ──────────────────
    //
    // Strategy: GROUP BY the leading PK column, computing per group:
    //   - count(*)
    //   - sum of a numeric hash derived from MD5 of the full composite PK
    //
    // Type-aware stringification ensures PG and CH produce identical text
    // for each PK value. Unsupported types (floats) are rejected upfront.

    let pk_columns = pk_types::build_pk_columns(pg, schema, table)?;
    let pk_cols: Vec<String> = pk_columns.iter().map(|c| c.name.clone()).collect();
    let lead_pk = &pk_columns[0].name;

    // Log PK types
    for col in &pk_columns {
        info!("  PK column: {} ({})", col.name, col.pg_type);
    }

    let pg_concat = pk_types::pg_concat_expr(&pk_columns);
    let ch_concat = pk_types::ch_concat_expr(&pk_columns);
    let pg_hash_expr = pk_types::pg_hash_agg(&pg_concat);
    let ch_hash_expr = pk_types::ch_hash_agg(&ch_concat);

    info!(
        "  PK hash comparison grouped by '{}' (PK: ({}))",
        lead_pk, pk_cols.join(", ")
    );

    // ── PG grouped query ────────────────────────────────────────────────
    info!("  [PG] querying grouped count + hash...");
    let t0 = Instant::now();
    let pg_groups = pg.query(&format!(
        "SELECT {}, count(*)::text, {}::text FROM {}.{} GROUP BY {} ORDER BY {}",
        pk_columns[0].pg_expr, pg_hash_expr, schema, table, lead_pk, lead_pk
    ))?;
    let pg_elapsed = t0.elapsed().as_secs();
    info!(
        "  [PG] {} groups ({}, {:.0} groups/s)",
        format_number(pg_groups.len() as i64),
        format_duration(pg_elapsed),
        if pg_elapsed > 0 { pg_groups.len() as f64 / pg_elapsed as f64 } else { 0.0 }
    );

    // ── CH grouped query ────────────────────────────────────────────────
    info!("  [CH] querying grouped count + hash...");
    let t0 = Instant::now();
    let ch_response = ch.query(&format!(
        "SELECT {}, toString(count()), toString({}) \
         FROM {} FINAL WHERE _pg2ch_is_deleted = 0 \
         GROUP BY {} ORDER BY {} FORMAT TabSeparated",
        pk_columns[0].ch_expr, ch_hash_expr, ch_table, lead_pk, lead_pk
    ))?;
    let ch_elapsed = t0.elapsed().as_secs();
    let ch_groups: Vec<Vec<&str>> = ch_response
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.split('\t').collect())
        .collect();
    info!(
        "  [CH] {} groups ({}, {:.0} groups/s)",
        format_number(ch_groups.len() as i64),
        format_duration(ch_elapsed),
        if ch_elapsed > 0 { ch_groups.len() as f64 / ch_elapsed as f64 } else { 0.0 }
    );

    // ── Build maps and compare ──────────────────────────────────────────
    // Key → (count, hash_sum)
    let mut pg_map: std::collections::BTreeMap<&str, (i64, &str)> = std::collections::BTreeMap::new();
    for row in &pg_groups {
        let count: i64 = row[1].parse().unwrap_or(0);
        pg_map.insert(&row[0], (count, &row[2]));
    }
    let mut ch_map: std::collections::BTreeMap<&str, (i64, &str)> = std::collections::BTreeMap::new();
    for row in &ch_groups {
        if row.len() >= 3 {
            let count: i64 = row[1].parse().unwrap_or(0);
            ch_map.insert(row[0], (count, row[2]));
        }
    }

    let all_keys: std::collections::BTreeSet<&str> = pg_map.keys().chain(ch_map.keys()).copied().collect();
    let total_groups = all_keys.len();

    info!("  comparing {} groups...", total_groups);
    let start = Instant::now();

    let mut matched_groups: i64 = 0;
    let mut matched_rows: i64 = 0;
    let mut pg_only_groups: Vec<(String, i64)> = Vec::new();
    let mut ch_only_groups: Vec<(String, i64)> = Vec::new();
    let mut count_mismatches: Vec<(String, i64, i64)> = Vec::new();
    let mut hash_mismatches: Vec<(String, i64, String, String)> = Vec::new(); // key, count, pg_hash, ch_hash
    let mut groups_processed: usize = 0;
    let mut last_progress = Instant::now();

    for key in &all_keys {
        let pg_entry = pg_map.get(key);
        let ch_entry = ch_map.get(key);

        match (pg_entry, ch_entry) {
            (Some((pg_c, _)), None) => {
                pg_only_groups.push((key.to_string(), *pg_c));
            }
            (None, Some((ch_c, _))) => {
                ch_only_groups.push((key.to_string(), *ch_c));
            }
            (Some((pg_c, pg_h)), Some((ch_c, ch_h))) => {
                if pg_c != ch_c {
                    count_mismatches.push((key.to_string(), *pg_c, *ch_c));
                } else if pg_h != ch_h {
                    hash_mismatches.push((key.to_string(), *pg_c, pg_h.to_string(), ch_h.to_string()));
                } else {
                    matched_groups += 1;
                    matched_rows += pg_c;
                }
            }
            (None, None) => unreachable!(),
        }

        groups_processed += 1;

        if last_progress.elapsed().as_secs() >= 10 {
            let pct = groups_processed as f64 / total_groups as f64 * 100.0;
            let elapsed = start.elapsed().as_secs();
            let eta = if pct > 0.0 {
                ((elapsed as f64 / pct * 100.0) - elapsed as f64) as u64
            } else {
                0
            };
            info!(
                "  progress: {:.1}% ({}/{} groups, ETA {})",
                pct, groups_processed, total_groups, format_duration(eta)
            );
            last_progress = Instant::now();
        }
    }

    let total_elapsed = start.elapsed().as_secs();
    let total_mismatched = pg_only_groups.len() + ch_only_groups.len() + count_mismatches.len() + hash_mismatches.len();

    info!(
        "  comparison done: {} groups ok ({} rows), {} mismatched ({})",
        matched_groups, format_number(matched_rows),
        total_mismatched, format_duration(total_elapsed)
    );

    if total_mismatched == 0 {
        return Ok(TableResult {
            table: table.clone(),
            level: DiffLevel::PrimaryKeys,
            status: DiffStatus::Match {
                detail: format!(
                    "all {} groups match ({} rows, {} distinct {})",
                    matched_groups, format_number(matched_rows), total_groups, lead_pk
                ),
            },
        });
    }

    // ── Report mismatches ───────────────────────────────────────────────
    let max_show = 20;

    if !pg_only_groups.is_empty() {
        let total_rows: i64 = pg_only_groups.iter().map(|(_, c)| c).sum();
        info!(
            "  {} in PG only: {} groups, {} rows",
            lead_pk, pg_only_groups.len(), format_number(total_rows)
        );
        for (key, count) in pg_only_groups.iter().take(max_show) {
            info!("    {}={} ({} rows)", lead_pk, key, count);
        }
        if pg_only_groups.len() > max_show {
            info!("    ... and {} more", pg_only_groups.len() - max_show);
        }
    }

    if !ch_only_groups.is_empty() {
        let total_rows: i64 = ch_only_groups.iter().map(|(_, c)| c).sum();
        info!(
            "  {} in CH only: {} groups, {} rows",
            lead_pk, ch_only_groups.len(), format_number(total_rows)
        );
        for (key, count) in ch_only_groups.iter().take(max_show) {
            info!("    {}={} ({} rows)", lead_pk, key, count);
        }
        if ch_only_groups.len() > max_show {
            info!("    ... and {} more", ch_only_groups.len() - max_show);
        }
    }

    if !count_mismatches.is_empty() {
        info!("  count mismatches: {} groups", count_mismatches.len());
        for (key, pg_c, ch_c) in count_mismatches.iter().take(max_show) {
            info!("    {}={}: PG {} / CH {} (diff {})", lead_pk, key, pg_c, ch_c, pg_c - ch_c);
        }
        if count_mismatches.len() > max_show {
            info!("    ... and {} more", count_mismatches.len() - max_show);
        }
    }

    if !hash_mismatches.is_empty() {
        info!(
            "  hash mismatches (same count, different PKs): {} groups",
            hash_mismatches.len()
        );
        for (key, count, pg_h, ch_h) in hash_mismatches.iter().take(max_show) {
            info!(
                "    {}={}: {} rows, PG hash {} / CH hash {}",
                lead_pk, key, count, pg_h, ch_h
            );
        }
        if hash_mismatches.len() > max_show {
            info!("    ... and {} more", hash_mismatches.len() - max_show);
        }
    }

    let pg_only_rows: i64 = pg_only_groups.iter().map(|(_, c)| c).sum();
    let ch_only_rows: i64 = ch_only_groups.iter().map(|(_, c)| c).sum();

    Ok(TableResult {
        table: table.clone(),
        level: DiffLevel::PrimaryKeys,
        status: DiffStatus::Mismatch {
            detail: format!(
                "{} of {} groups differ: {} PG-only ({} rows), {} CH-only ({} rows), {} count mismatches, {} hash mismatches",
                total_mismatched, total_groups,
                pg_only_groups.len(), format_number(pg_only_rows),
                ch_only_groups.len(), format_number(ch_only_rows),
                count_mismatches.len(), hash_mismatches.len()
            ),
        },
    })
}
