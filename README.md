# pg2ch_cdc

rsync-like PostgreSQL to ClickHouse CDC replicator. Runs once, syncs everything, exits.

```
PostgreSQL (pgoutput WAL) ──► pg2ch_cdc (libpq FFI) ──► ClickHouse (HTTP TabSeparated)
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
4. Snapshots WAL position before any loads
5. For each table, compares source and destination:
   - Missing in ClickHouse → auto-create table from PG schema
   - Empty in ClickHouse → initial load via `INSERT INTO ... SELECT FROM postgresql()`
   - Partial load detected → truncate and reload
   - Already loaded → skip to CDC
6. Drains all pending WAL changes via logical replication
7. Post-CDC integrity check on newly loaded tables
8. Exits

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

Uses ClickHouse's `postgresql()` table function for direct bulk transfer — ClickHouse pulls data straight from PostgreSQL with no intermediate staging. Tables are loaded in parallel (configurable via `parallel_loads`).

For tables over 1M rows, a progress monitor shows rows loaded, throughput, and ETA every 60 seconds.

### CDC (Change Data Capture)

Connects to PostgreSQL's logical replication protocol using the `pgoutput` plugin. Reads the WAL stream, parses binary pgoutput messages (Insert, Update, Delete), and batches changes into ClickHouse via HTTP POST in TabSeparated format.

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

pg2ch_cdc does **not** use `REPEATABLE READ` transactions for initial loads. Instead:

1. Snapshots the WAL position (`pg_current_wal_flush_lsn()`) **before** any loads
2. Each table load runs independently via `postgresql()` (each gets its own snapshot)
3. CDC replays from the pre-load LSN, catching any changes made during loading
4. ReplacingMergeTree deduplication makes overlapping rows harmless

This avoids holding a long transaction open (which blocks PostgreSQL vacuuming) while guaranteeing no data gaps.

### Partial load detection

If a load crashes mid-way, the next run detects it:
- `max(_pg2ch_version) = 0` (no CDC has ever processed this table)
- `count()` is less than 80% of `pg_class.reltuples`

Detected partial loads are truncated and reloaded automatically.

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

- **PG rows(est)**: Estimated row count from `pg_class.reltuples` (instant, no table scan)
- **CH rows**: `✓` means rows exist (skips exact count), `0` means empty, `PARTIAL` means incomplete load detected
- **Actions**: `CREATE + LOAD` (new table), `LOAD` (empty), `RELOAD (partial)` (truncate + reload), `CDC` (already loaded), `SKIP` (empty on both sides)

### CDC progress

During WAL processing, a progress line is logged every 10 seconds:

```
CDC [4m34s] 63.4% — 905.9k msgs (3.3k/s, 150846I/149461U/1708D) [flushing 0.47 GB buffered, PG at 24.07 GB remaining]
```

| Field | Meaning |
|-------|---------|
| `[4m34s]` | Elapsed time since CDC started |
| `63.4%` | WAL progress — how far through the WAL range we are (from confirmed LSN to target LSN) |
| `905.9k msgs` | Total pgoutput WAL messages received from PostgreSQL. Each insert, update, or delete on a published table generates one message. This count includes all messages for tables in the publication. |
| `3.3k/s` | Average message throughput since CDC started |
| `150846I/149461U/1708D` | Breakdown of applied changes: Inserts, Updates, Deletes |
| `[flushing ...]` or `[PG decoding ...]` | Current bottleneck state (see below) |

#### Bottleneck states

CDC alternates between two states depending on where the bottleneck is:

**PG decoding** — PostgreSQL is scanning WAL server-side, we are idle:
```
CDC [10s] 11.3% — 0.0k msgs (0.0k/s, 0I/0U/0D) [PG decoding: 4.78 GB remaining]
```
PostgreSQL reads WAL sequentially and runs the `pgoutput` plugin on each record. Records for tables not in the publication are skipped server-side. During this phase, our process receives no data — we are waiting for PG.

The `remaining` value comes from `pg_stat_replication.sent_lsn`, showing how far PG has decoded relative to our target LSN.

**Flushing** — PostgreSQL has sent a burst of data, we are busy inserting into ClickHouse:
```
CDC [5m04s] 67.2% — 905.9k msgs (3.0k/s, 150846I/149461U/1708D) [flushing 2.97 GB buffered, PG at 21.57 GB remaining]
```
PG sent data faster than we can process it. The `buffered` value shows how much data is queued in the libpq/kernel buffers waiting for us to consume. PG continues decoding ahead while we flush.

When the message count stops increasing but the buffered value grows, it means PG is sending WAL that contains no relevant changes for our publication — it is racing through irrelevant WAL while we have nothing to do.

#### Why the message count can be much larger than I+U+D

The `msgs` count includes **all** pgoutput protocol messages: Relation (schema metadata), Begin/Commit (transaction boundaries), and data messages (Insert/Update/Delete). Only data messages for tables that have a matching CDC batch are counted in I/U/D. Messages for tables in the publication but not in your config are received and discarded.

### Initial load progress

For tables with over 1M estimated rows, a progress monitor logs every 60 seconds:

```
[W0] orders progress: 45.2% (2260000/5000000 rows, 37666 rows/s, ETA 1m)
```

The target count comes from `pg_class.reltuples` (approximate). The actual row count comes from ClickHouse `count()` which is instant on MergeTree engines.

## PostgreSQL requirements

- Source tables **must have primary keys** (the tool validates this at startup)
- The PostgreSQL user must have **replication privileges**
- Uses the `pgoutput` logical decoding plugin (built into PostgreSQL 10+)

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
