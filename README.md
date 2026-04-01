# pg2ch_cdc

rsync-like PostgreSQL to ClickHouse CDC replicator. Runs once, syncs everything, exits.

```
PostgreSQL ──► pg2ch_cdc ──► ClickHouse
```

---

# Usage

## What it does

```bash
pg2ch_cdc --config mirrors/my_mirror.yaml
```

1. Connects to PostgreSQL and ClickHouse
2. Validates all source tables have primary keys
3. Creates publication and replication slot if missing
4. For each table, compares source and destination:
   - Missing in ClickHouse → auto-create table
   - Empty in ClickHouse → initial bulk load
   - Partial load detected → truncate and reload
   - Already loaded → skip to CDC
5. Applies all pending changes (inserts, updates, deletes) from the WAL
6. Exits

Run it on a schedule. Every run picks up where the last one left off. Safe to kill mid-run.

## Quick start

### Configure

Create a YAML config in `mirrors/`:

```yaml
mirror_name: my_mirror

source:
  host: pg-host
  port: 5432
  database: my_database
  user: replication_user
  password: secret
  schema: public

destination:
  host: ch-host
  port: 8123
  database: my_ch_database
  user: default
  password: secret

settings:
  batch_size: 1000           # CDC batch size before flush
  flush_interval_secs: 5     # max seconds between CDC flushes
  parallel_loads: 2          # concurrent initial load threads
  binary: false              # pgoutput binary mode (PG 14+)
  ch_timeout_secs: 21600     # HTTP timeout for long-running loads

tables:
  - users
  - orders
  - products
```

### Run

```bash
# Normal run (with tracing timestamps)
./target/release/pg2ch_cdc --config mirrors/my_mirror.yaml

# Cron / Airflow (plain output, no ANSI)
./target/release/pg2ch_cdc --config mirrors/my_mirror.yaml --plain
```

## How it works

### Initial load

Bulk-loads data directly from PostgreSQL into ClickHouse with no intermediate staging. Tables are loaded in parallel (configurable via `parallel_loads`).

For tables over 1M rows, a progress monitor shows rows loaded, throughput, and ETA every 60 seconds.

### CDC (Change Data Capture)

After the initial load, applies all changes (inserts, updates, deletes) that occurred in PostgreSQL since the load started. On subsequent runs, picks up from where the last run left off — only new changes are applied.

### ClickHouse table schema

Every target table is auto-created as a `ReplacingMergeTree` with three extra columns:

```sql
CREATE TABLE my_table (
    -- ... source columns (auto-mapped from PG types) ...
    _pg2ch_synced_at DateTime64(9) DEFAULT now64(),
    _pg2ch_is_deleted UInt8 DEFAULT 0,
    _pg2ch_version UInt64 DEFAULT 0
) ENGINE = ReplacingMergeTree(_pg2ch_version, _pg2ch_is_deleted)
ORDER BY (primary_key_columns);
```

- **INSERT/UPDATE**: New row with `_pg2ch_is_deleted=0`, incrementing `_pg2ch_version`
- **DELETE**: Row with `_pg2ch_is_deleted=1` — filtered out by `FINAL`

Query with deduplication:
```sql
SELECT * FROM my_table FINAL WHERE _pg2ch_is_deleted = 0
```

### Consistency model

pg2ch_cdc records the WAL position before starting any loads. After all tables are loaded, CDC replays from that position, catching any changes made during loading. This guarantees no data gaps without holding long transactions open on PostgreSQL.

### Partial load detection

If a load crashes mid-way, the next run detects it and automatically truncates and reloads the table.

### Dropped and recreated tables

If an external process drops and recreates a source table (e.g. a data loader that does `DROP TABLE` / `CREATE TABLE` as part of a nightly refresh), pg2ch_cdc handles it automatically. The table is detected as missing from the publication on the next run, re-added, and the ClickHouse side is truncated and reloaded with the new data. Stale WAL entries from the old table are safely ignored.

## Understanding the output

### Table status

At startup, pg2ch_cdc prints a diff of every table:

```
TABLE                          PG rows(est)      CH rows  ACTION
────────────────────────────────────────────────────────────────────
users                                 1500            0  LOAD
orders                             5000000            ✓  CDC
products                               800            ✓  CDC
audit_log                                0            0  SKIP (empty)
```

- **PG rows(est)**: Estimated row count from PostgreSQL statistics (instant, no table scan)
- **CH rows**: `✓` means rows exist, `0` means empty, `PARTIAL` means incomplete load detected
- **Actions**: `CREATE + LOAD` (new table), `LOAD` (empty), `RELOAD (partial)` (truncate + reload), `CDC` (already loaded), `SKIP` (empty on both sides)

### CDC progress

During WAL processing, a progress line is logged every 10 seconds:

```
CDC [4m34s] 63.4% — 905.9k msgs (3.3k/s, 150846I/149461U/1708D) [flushing 0.47 GB buffered, PG at 24.07 GB remaining]
```

| Field | Meaning |
|-------|---------|
| `[4m34s]` | Elapsed time since CDC started |
| `63.4%` | WAL progress — how far through the pending changes we are |
| `905.9k msgs` | Total WAL messages received from PostgreSQL |
| `3.3k/s` | Average message throughput since CDC started |
| `150846I/149461U/1708D` | Breakdown of applied changes: Inserts, Updates, Deletes |
| `[flushing ...]` or `[PG decoding ...]` | Current bottleneck state (see below) |

#### Bottleneck states

CDC alternates between two states depending on where the bottleneck is:

**PG decoding** — waiting for PostgreSQL to read and send WAL data:
```
CDC [10s] 11.3% — 0.0k msgs (0.0k/s, 0I/0U/0D) [PG decoding: 4.78 GB remaining]
```

**Flushing** — receiving data from PostgreSQL, writing to ClickHouse:
```
CDC [5m04s] 67.2% — 905.9k msgs (3.0k/s, 150846I/149461U/1708D) [flushing 2.97 GB buffered, PG at 21.57 GB remaining]
```

The `msgs` count may be larger than I+U+D because it includes protocol overhead (transaction boundaries, schema metadata) and changes for tables in the publication but not in your config.

### Initial load progress

For tables with over 1M estimated rows, a progress monitor logs every 60 seconds:

```
[W0] orders progress: 45.2% (2260000/5000000 rows, 37666 rows/s, ETA 1m)
```

The target count is an estimate from PostgreSQL statistics. The loaded count is read from ClickHouse in real time.

## PostgreSQL requirements

- Source tables **must have primary keys** (the tool validates this at startup)
- The PostgreSQL user must have **replication privileges**
- PostgreSQL 10+ (uses built-in logical replication)

---

# pg2ch_diff — Data validation tool

Validates that ClickHouse mirrors faithfully reflect PostgreSQL source tables. Compares data cross-database using progressive validation levels — from instant metadata checks to full row-level checksums on billion-row tables.

```bash
pg2ch_diff --config diffs/my_diff.yaml
```

## How it works

The key insight: ClickHouse is fast, PostgreSQL is the bottleneck. Rather than hashing rows on both sides independently, pg2ch_diff snapshots the PG table into a temporary ClickHouse table via `postgresql()`, then compares entirely within ClickHouse using `FULL JOIN` + `sipHash64`. This avoids expensive PG table scans for hash computation.

### Validation levels

Each table can be configured with a different validation level. Higher levels give more information but cost more.

| Level | What it checks | PG load | CH load |
|---|---|---|---|
| `metadata_count` | `pg_class.reltuples` vs `count()` without FINAL | Instant | Instant |
| `exact_count` | `count(*)` on both sides | Full seq scan | Moderate (FINAL) |
| `primary_keys` | PK set comparison via snapshot + sipHash64 on PK columns | Snapshot via `postgresql()` | FULL JOIN |
| `checksum` | Full row comparison via snapshot + sipHash64 on all columns | Snapshot via `postgresql()` | FULL JOIN |

### Snapshot + compare approach (levels 3-4)

1. Record `max(_pg2ch_version)` on the CDC table (stability marker)
2. Snapshot PG into a temp CH table: `INSERT INTO temp._diff_{table} SELECT * FROM postgresql(...)`
3. Compare snapshot vs CDC table in CH using chunked `FULL JOIN` with `sipHash64`
4. Drill down hash mismatches with rounding fallback (catches float ULP noise)
5. Verify `_pg2ch_version` didn't change during the process
6. Drop temp table (unless `--keep-snapshot`)

### Chunked comparison

For large tables (billions of rows), a single `FULL JOIN` would exceed memory. The tool splits the comparison into chunks of ~20M rows each, based on the leading primary key.

Chunk boundaries are computed using ClickHouse's approximate quantile function (`quantilesGK`), which scans the snapshot once without sorting. The accuracy parameter scales with the number of chunks: `max(100, num_chunks * 2)`.

Progress is reported per chunk:

```
[chunk 3/250]  20.1M rows  OK  1.2%  ETA 45m  (12s)
[chunk 4/250]  19.8M rows  OK  1.6%  ETA 44m  (11s)
```

### Float handling

The `postgresql()` table function can introduce small ULP (unit in the last place) differences in floating-point values compared to CDC. The tool handles this in two stages:

1. **Primary comparison**: bit-masks the last N mantissa bits of floats before hashing (Float32: 6 bits, Float64: 14 bits)
2. **Rounding fallback**: rows that still mismatch are re-checked with `round()` to catch mantissa overflow cases

Rows that match after rounding are reported as "ULP noise" (not real mismatches).

### Decimal tolerance

The snapshot can also introduce small rounding errors in `Decimal` columns at the scale boundary. Set `decimal_tolerance` in the config to absorb this:

```yaml
decimal_tolerance: 0.0001  # tolerates ±0.0001 difference
```

## Diff config

Diff configs live in `diffs/`. They reuse the same source/destination format as mirror configs, with per-table validation levels.

```yaml
mirror_name: my_mirror

source:
  host: pg-host
  port: 5432
  database: my_database
  user: my_user
  password: secret
  schema: public

destination:
  host: ch-host
  port: 8123
  database: my_ch_database
  user: default
  password: secret

# Optional: separate database for temporary snapshot tables (default: destination database)
temp_database: temp

# Optional: ClickHouse HTTP timeout in seconds (default: 86400 = 24h)
ch_timeout_secs: 86400

# Optional: tolerance for Decimal column rounding noise
decimal_tolerance: 0.0001

tables:
  - name: small_table
    level: exact_count
  - name: big_table
    level: checksum
  - name: append_only_table
    level: primary_keys
```

## CLI flags

| Flag | Description |
|---|---|
| `--config <path>` | Path to the YAML diff config |
| `--plain` | Plain output without timestamps (for Airflow/cron) |
| `--skip-snapshot` | Reuse existing snapshot table from a previous run (errors if not found) |
| `--keep-snapshot` | Don't drop the snapshot table after comparison |

`--keep-snapshot` and `--skip-snapshot` are useful together: first run creates and keeps the snapshot, subsequent runs reuse it to iterate on comparison without re-reading PG.

## Output

The tool prints per-table progress and a summary:

```
═══ Summary ═══════════════════════════════════════════════════
TABLE                               LEVEL  RESULT
──────────────────────────────────────────────────────────────────────
small_table                    ExactCount  OK — both have 1.2M rows
big_table                        Checksum  OK — 2.1B matching, 0 mismatches
append_only_table              PrimaryKeys  OK — 500.0M matching, 0 mismatches
──────────────────────────────────────────────────────────────────────
3 tables checked: 3 ok, 0 mismatches, 0 errors
```

Exit code is 0 if all tables match, 1 if any mismatch or error.

## Source files

| File | Purpose |
|---|---|
| `src/bin/pg2ch_diff/main.rs` | CLI parsing, tracing init, summary output |
| `src/bin/pg2ch_diff/config.rs` | YAML config with per-table diff levels |
| `src/bin/pg2ch_diff/diff.rs` | Diff engine — snapshot, chunked comparison, hash drilldown |
| `src/bin/pg2ch_diff/col_types.rs` | Type-aware column expressions for cross-database hash comparison |

## Naming conventions

- Publication: `pg2ch_{mirror_name}`
- Replication slot: `pg2ch_{mirror_name}`
- ClickHouse table: `{destination.database}.{table_name}`

---

# Contributing

## Design choices

- **libpq FFI** for PostgreSQL — the replication protocol requires it
- **HTTP + TabSeparated** for ClickHouse — dynamic columns without compile-time Row structs
- **Fully synchronous** — no async runtime, single-threaded `poll()` loop
- **Stateless** — all state is derived from source and destination, no metadata tables
- **Type mapping** via `DESCRIBE TABLE postgresql()` — ClickHouse performs the PG→CH type conversion itself, so we never maintain a manual mapping

## Building

```bash
# Requires: rust toolchain, clang-devel, postgresql-devel
BINDGEN_EXTRA_CLANG_ARGS="-I/usr/include/pgsql" cargo build --release
```

### Build dependencies

- Rust toolchain (`rustc` / `cargo` via [rustup](https://rustup.rs/))
- `clang-devel` (for `libpq-sys` bindgen)
- `postgresql-devel` (for libpq headers)
- On openSUSE: `BINDGEN_EXTRA_CLANG_ARGS="-I/usr/include/pgsql"`

### Runtime dependencies

The binary dynamically links against system libraries:

- `libpq.so.5` — PostgreSQL client library
- `libssl.so` / `libcrypto.so` — OpenSSL
- `libgssapi_krb5.so` — Kerberos (via libpq)
- `libldap.so` — LDAP (via libpq)
- `libz.so` — zlib compression
- `libc.so.6` — glibc

The target machine must have these libraries installed. On most Linux distributions, installing `postgresql-libs` (or `libpq5`) and `openssl-libs` covers the non-standard dependencies.

## Source files

| File | Purpose |
|------|---------|
| `src/main.rs` | CLI argument parsing, config loading, entry point |
| `src/orchestrator.rs` | Table diffing, initial loads, CDC orchestration, integrity checks |
| `src/cdc.rs` | CDC event loop — `poll()`, WAL consumption, progress monitoring |
| `src/pgoutput.rs` | Pure Rust pgoutput binary protocol parser (Relation/Insert/Update/Delete/Begin/Commit) |
| `src/pg.rs` | PostgreSQL client (libpq FFI wrapper, two methods: `execute`, `query`) |
| `src/clickhouse.rs` | ClickHouse HTTP client and CDC batch accumulator |
| `src/config.rs` | YAML config deserialization |

## License

MIT
