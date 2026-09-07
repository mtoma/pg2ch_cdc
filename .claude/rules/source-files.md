# Source files

| File | Purpose |
|------|---------|
| `main.rs` | CLI (`--config`, `--plain`), tracing init, entry point |
| `config.rs` | YAML config parsing (serde), mandatory `timezone:` validation, publication/slot/table naming |
| `orchestrator.rs` | Diff report, create tables, parallel initial loads with progress monitoring, snapshot LSN, partial load detection, drain CDC, post-CDC integrity check |
| `cdc.rs` | Replication protocol: start slot, read WAL, multi-table message routing, drain to target LSN |
| `clickhouse.rs` | HTTP client (pins `session_timezone` on every request), CdcBatch accumulator, TSV escaping, DateTime type-string timezone pinning |
| `pg.rs` | Thin libpq wrapper: `execute(sql)` and `query(sql) → Vec<Vec<String>>` |
| `pgoutput.rs` | Binary pgoutput protocol parser (Relation/Insert/Update/Delete/Begin/Commit) |
| `types.rs` | PG→CH type conversion: text mode, binary wire format, numeric. Timestamps are forwarded verbatim — ClickHouse resolves them |

## pg2ch_diff (validation tool)

| File | Purpose |
|------|---------|
| `src/bin/pg2ch_diff/main.rs` | CLI (`--config`, `--plain`, `--skip-snapshot`, `--keep-snapshot`), summary output |
| `src/bin/pg2ch_diff/config.rs` | Diff-specific YAML config: per-table diff levels, temp_database, decimal_tolerance |
| `src/bin/pg2ch_diff/diff.rs` | Diff engine: snapshot PG via `postgresql()`, chunked `FULL JOIN` + `sipHash64`, rounding drilldown |
| `src/bin/pg2ch_diff/col_types.rs` | Type-aware stringify expressions: float bit-masking, timestamp normalization, NULL handling |

## YAML config

One file per mirror in `mirrors/`. Example:

```yaml
mirror_name: cstat     # used for publication/slot naming: pg2ch_cstat
store_naive_timestamps_as_timezone: UTC   # defaults to UTC

source:
  host: pg-host
  port: 5432
  database: mydb
  user: replication_user
  password: secret
  schema: cstat

destination:
  host: ch-host
  port: 8123
  database: mydb_mirror
  user: default
  password: secret

settings:
  batch_size: 1000           # CDC batch size before flush
  flush_interval_secs: 5     # max seconds between flushes
  parallel_loads: 2          # concurrent initial load threads
  binary: false              # pgoutput binary mode (PG 14+)
  ch_timeout_secs: 21600     # HTTP timeout for CH queries (6h for large tables)

tables:
  - table_a
  - table_b
  # - table_c              # commented = excluded
```
