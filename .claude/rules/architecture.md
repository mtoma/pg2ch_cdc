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
_pg2ch_synced_at DateTime64(9, '<timezone>') DEFAULT now64()
_pg2ch_is_deleted UInt8 DEFAULT 0
_pg2ch_version UInt64 DEFAULT 0
```

Engine is always `ReplacingMergeTree(_pg2ch_version, _pg2ch_is_deleted)` with `ORDER BY (pk_columns)`.

To query with correct deduplication: `SELECT ... FROM table FINAL WHERE _pg2ch_is_deleted = 0`.

## Type mapping

CH types are determined by ClickHouse's own `postgresql()` table function via `DESCRIBE TABLE postgresql(...)`. This ensures the mapping is always consistent with what CH would produce natively. Nullable PG columns become `Nullable()` in CH. PK columns are never Nullable.

There are **two** post-processing steps. Neither is a type mapping — CH still
decides Int32 vs Decimal vs String vs DateTime64, and there is no mapping table
to maintain. Both only qualify a type CH already chose, and both must be applied
in the same two places: `create_ch_table`, and the drift check's normalisation
of `DESCRIBE` output. Miss the second and the adjusted column reads as drift
forever, so the next load silently reverts it.

1. **Timezone pinning.** `DESCRIBE` always returns a bare `DateTime64(6)`, which
   silently binds the column to the ClickHouse server default.
   `clickhouse::pin_datetime_timezone` writes the config's
   `store_naive_timestamps_as_timezone` into whatever DateTime type CH chose.
   See `timezones.md`.

2. **Widening the unconstrained decimal.** A PG `numeric` with no declared
   precision is arbitrary-precision; CH maps it to `Decimal(38, 19)`, which
   holds only 19 digits *before* the point. A source value >= 10^19 then fails
   the INSERT with `Code: 69 ... Decimal value is too big` — and that aborts the
   whole run, not just the row. `clickhouse::widen_unconstrained_decimal`
   rewrites exactly `Decimal(38, 19)` to `Decimal(76, 19)` (Decimal256, 57
   integer digits). That shape is precisely the unconstrained-numeric signature,
   since a declared `numeric(p,s)` maps to `Decimal(p,s)`, so declared
   precisions are left alone.

   On 2026-09-14 one row of `cstat.sec_dtrt` (gvkey 108893,
   `trfd = 17485804441876462000`, in a column whose values are otherwise ~1.0)
   blocked `cdc_cstat` for 3h31m — 13 failed attempts across 7 scheduled runs —
   and left its slot 74 GB behind. One implausible row in 2,382,042 stopped a 149-table
   mirror. Widening does not make overflow impossible, only implausible: beyond
   57 integer digits it still fails, which is right — truncating a number the
   source really holds would be worse than stopping.

CDC type conversions handled in `types.rs`:
- `bool` → `UInt8` (t/f → 1/0)
- `timestamp` / `timestamptz` → forwarded verbatim; ClickHouse resolves them
  (`session_timezone` + `date_time_input_format=best_effort`). See `timezones.md`.
- `numeric` → `Decimal` (binary base-10000 decoder)
- Binary mode: int2/4/8, float4/8, date, timestamp, uuid all decoded from PG wire format

## Delete handling

PG default replica identity sends only PK columns in DELETE messages. The tool fills non-PK columns with type-appropriate defaults (0 for numbers, "" for strings, epoch for timestamps). The actual values don't matter — only the PK and `_pg2ch_is_deleted=1` are significant for ReplacingMergeTree.

## Naming conventions

- Publication: `pg2ch_{mirror_name}` (e.g. `pg2ch_cstat`)
- Replication slot: `pg2ch_{mirror_name}` (e.g. `pg2ch_cstat`)
- CH table: `{destination.database}.{table_name}` (same name as PG source table)

## Replication slot advancement

A logical slot pins every WAL segment after its `restart_lsn`, and `restart_lsn`
cannot pass what the client confirms. A client that only confirms changes it
decoded **for its own publication** therefore pins all the WAL in between —
including WAL belonging entirely to other tables.

With several mirrors on one busy database this is not a corner case, it is the
normal state. On 2026-09-04 a ~850 GB burst on the `ciq` tables held `cdc_ciq`
for 2h49m; the serialised DAG meant `cdc_fds` never ran, its slot froze at
`5CB7/8F0CC320`, PostgreSQL retained **2.1 TB** of WAL across 136,169 segments,
and the source database filled its disk and shut down:

```
FATAL: could not extend file "base/16413/1196228965": No space left on device
LOG: shutting down due to startup process failure
```

Two things address it, and both are needed:

1. **Keepalive-driven advancement** (`cdc.rs`, "Keepalive-driven advancement").
   We confirm up to the `walEnd` of primary keepalives, not just our last
   decoded change, so a slot walks through WAL that holds nothing for it. Safe
   because a logical walsender's `walEnd` is its own decoding position — see the
   module docs for the walsender source that proves it. Guarded by the same
   flush-before-promote ordering as everything else.
2. **Concurrent mirrors** (Airflow `max_active_tasks=3`, no task dependencies).
   Serialised tasks meant one slow mirror denied the others the chance to
   acknowledge at all, which no amount of client-side cleverness can fix.

`tests/test_slot_advance_unrelated_wal.sh` is the regression test: one row in the
publication, a large volume of churn in a table outside it, and an assertion that
the slot advances anyway.

Note that `restart_lsn` moves in steps, not continuously — PostgreSQL only
advances it when `candidate_restart_valid` is set while decoding an
`XLOG_RUNNING_XACTS` record, so retention is released at those boundaries even
when `confirmed_flush_lsn` is fully up to date.
