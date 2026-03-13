# Architecture

```
PostgreSQL (pgoutput WAL) ──► pg2ch_cdc (libpq FFI) ──► ClickHouse (HTTP TabSeparated)
```

- **libpq**: C library via FFI — handles replication protocol, SSL, COPY_BOTH mode
- **ClickHouse**: Raw HTTP POST with `reqwest::blocking` — TabSeparated format
- **Fully synchronous**: No tokio, no async — single-threaded, simple
- **ReplacingMergeTree**: Initial load with `_pg2ch_version=0`, CDC with monotonically increasing versions. `FINAL` resolves duplicates. Deletes marked with `_pg2ch_is_deleted=1`.

## ClickHouse target table schema

Auto-created by the orchestrator. Every table gets three extra columns:

```sql
_pg2ch_synced_at DateTime64(9) DEFAULT now64()
_pg2ch_is_deleted UInt8 DEFAULT 0
_pg2ch_version UInt64 DEFAULT 0
```

Engine is always `ReplacingMergeTree(_pg2ch_version, _pg2ch_is_deleted)` with `ORDER BY (pk_columns)`.

To query with correct deduplication: `SELECT ... FROM table FINAL WHERE _pg2ch_is_deleted = 0`.

## Type mapping

CH types are determined by ClickHouse's own `postgresql()` table function via `DESCRIBE TABLE postgresql(...)`. This ensures the mapping is always consistent with what CH would produce natively. Nullable PG columns become `Nullable()` in CH. PK columns are never Nullable.

CDC type conversions handled in `types.rs`:
- `bool` → `UInt8` (t/f → 1/0)
- `timestamptz` → `DateTime64(6)` (converted to UTC, offset stripped)
- `numeric` → `Decimal` (binary base-10000 decoder)
- Binary mode: int2/4/8, float4/8, date, timestamp, uuid all decoded from PG wire format

## Delete handling

PG default replica identity sends only PK columns in DELETE messages. The tool fills non-PK columns with type-appropriate defaults (0 for numbers, "" for strings, epoch for timestamps). The actual values don't matter — only the PK and `_pg2ch_is_deleted=1` are significant for ReplacingMergeTree.

## Naming conventions

- Publication: `pg2ch_{mirror_name}` (e.g. `pg2ch_cstat`)
- Replication slot: `pg2ch_{mirror_name}` (e.g. `pg2ch_cstat`)
- CH table: `{destination.database}.{table_name}` (same name as PG source table)
