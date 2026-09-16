//! Measure the CDC serialisation path: pgoutput tuples -> TabSeparated payload.
//!
//! Models `cstat.sec_dprc` — 18 columns, the table whose 476M-row reload was in
//! the backlog on 2026-09-16 — and runs the same code the CDC loop runs, in
//! batches of 100,000 as configured for that mirror.
//!
//! Reports rows/s and peak RSS. RSS matters as much as throughput here: the
//! version this replaced retained every row as `Vec<Vec<Option<String>>>` until
//! flush and the process sat at 6.2 GB.
//!
//!     cargo run --release --example bench_serialise [total_rows]

use std::time::{Duration, Instant};

use pg2ch_cdc::clickhouse::{CdcBatch, RowKind};
use pg2ch_cdc::pgoutput::{ColumnInfo, RelationInfo, TupleData};
use pg2ch_cdc::types::write_tuple_into;

/// sec_dprc: gvkey, iid, datadate, curcdd, then 14 numeric/smallint columns.
fn sec_dprc_relation() -> RelationInfo {
    let mut columns = vec![
        ColumnInfo { name: "gvkey".into(),    flags: 1, type_oid: 1043, type_modifier: -1 },
        ColumnInfo { name: "iid".into(),      flags: 1, type_oid: 1043, type_modifier: -1 },
        ColumnInfo { name: "datadate".into(), flags: 1, type_oid: 1114, type_modifier: -1 },
        ColumnInfo { name: "curcdd".into(),   flags: 1, type_oid: 1043, type_modifier: -1 },
    ];
    for n in ["adrrc", "ajexdi", "cshoc", "cshtrd", "dvi", "eps", "prccd", "prchd",
              "prcld", "prcod", "qunit"] {
        columns.push(ColumnInfo { name: n.into(), flags: 0, type_oid: 1700, type_modifier: -1 });
    }
    for n in ["epsmo", "prcstd"] {
        columns.push(ColumnInfo { name: n.into(), flags: 0, type_oid: 21, type_modifier: -1 });
    }
    columns.push(ColumnInfo { name: "pacvertofeedpop".into(), flags: 0, type_oid: 23, type_modifier: -1 });
    RelationInfo { id: 54146, namespace: "cstat".into(), name: "sec_dprc".into(),
                   replica_identity: b'd', columns }
}

/// One row's worth of pgoutput values. A tenth are NULL, matching the sparse
/// numeric columns in the real table; one column carries a backslash so the
/// escaping is exercised rather than skipped.
fn sample_row(i: usize) -> Vec<TupleData> {
    let mut v = vec![
        TupleData::Text(format!("{:06}", i % 400_000)),
        TupleData::Text("01".to_string()),
        TupleData::Text("2026-09-16 00:00:00".to_string()),
        TupleData::Text("USD".to_string()),
    ];
    for c in 0..11 {
        if (i + c) % 10 == 0 {
            v.push(TupleData::Null);
        } else if c == 3 {
            v.push(TupleData::Text("a\\b".to_string()));
        } else {
            v.push(TupleData::Text(format!("{}.{:08}", i % 1000, (i * 7 + c) % 100_000_000)));
        }
    }
    v.push(TupleData::Text("12".to_string()));
    v.push(TupleData::Text("3".to_string()));
    v.push(TupleData::Text(format!("{}", i)));
    v
}

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1).map(|v| v.parse().ok()))
                .flatten()
        })
        .unwrap_or(0)
}

fn main() {
    let total: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(2_000_000);
    const BATCH: usize = 100_000;

    let rel = sec_dprc_relation();
    let cols: Vec<String> = rel.columns.iter().map(|c| c.name.clone()).collect();
    // Pre-build the tuples so the measurement is serialisation, not formatting
    // of test data.
    let rows: Vec<Vec<TupleData>> = (0..BATCH).map(sample_row).collect();

    let mut batch = CdcBatch::new("db.sec_dprc".to_string(), cols, BATCH, Duration::from_secs(3600));
    batch.set_rel_id(54146);

    let mut bytes: u64 = 0;
    let started = Instant::now();
    let mut done = 0usize;
    while done < total {
        for r in &rows {
            batch.add_row(RowKind::Insert, |buf| write_tuple_into(buf, r, &rel));
            done += 1;
            if batch.pending_count() >= BATCH {
                // What flush() would POST, then the same reset, without a server.
                let payload = batch.tsv_payload();
                bytes += payload.len() as u64;
                if let Ok(path) = std::env::var("BENCH_DUMP") {
                    if !std::path::Path::new(&path).exists() {
                        std::fs::write(&path, payload.as_bytes()).unwrap();
                    }
                }
                batch.discard_pending();
            }
            if done >= total { break; }
        }
    }
    let elapsed = started.elapsed();

    let secs = elapsed.as_secs_f64();
    println!("rows            {}", done);
    println!("elapsed         {:.3}s", secs);
    println!("throughput      {:.0} rows/s", done as f64 / secs);
    println!("payload         {:.1} MiB", bytes as f64 / 1_048_576.0);
    println!("bytes/row       {:.0}", bytes as f64 / done as f64);
    println!("peak RSS        {:.1} MiB", peak_rss_kb() as f64 / 1024.0);
}
