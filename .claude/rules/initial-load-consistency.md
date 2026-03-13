# Initial load consistency model

Other CDC tools (PeerDB, Debezium) wrap all initial loads in a single `REPEATABLE READ` transaction so every table sees the same point-in-time snapshot. We deliberately do **not** do this.

## Why not REPEATABLE READ

Our loads use ClickHouse's `postgresql()` table function — ClickHouse opens its own connections to PostgreSQL, and there is no way to make those connections join a transaction we hold on our side. Even if we could (e.g. via `pg_export_snapshot()`), holding a transaction open for hours (CIQ: 6h+, CSTAT: 2h+) prevents PostgreSQL from vacuuming any rows visible to that snapshot, causing serious table bloat on production databases.

## What we do instead (Option 3 — snapshot LSN before loads)

1. Snapshot `pg_current_wal_flush_lsn()` **before** any initial loads
2. Each `postgresql()` call opens its own connection with its own snapshot (each is ≥ the pre-load LSN)
3. CDC replays from the pre-load LSN, catching any changes made during loading
4. ReplacingMergeTree deduplication makes overlapping rows harmless (highest `_pg2ch_version` wins)

This guarantees **no gaps** — only overlaps, which are safe. Each table may see a slightly different point in time, but CDC resolves any differences within seconds of completing the loads.

## Partial load detection

If a load crashes mid-way, the next run detects it via `max(_pg2ch_version) = 0` (no CDC has ever run) AND `count() < 80% of pg_class.reltuples` estimate. Detected partial loads are truncated and reloaded. All `count()`/`max()` queries use `SETTINGS final = 0` for instant metadata reads (no ReplacingMergeTree deduplication).

## When REPEATABLE READ would matter

Only if you need cross-table consistency at a single point in time during the brief window between load completion and CDC catch-up. For our use case (analytics warehouse, not transactional reads), this is not a concern.

## Why REPEATABLE READ can't be easily added

The `postgresql()` table function is what makes loads fast and simple — ClickHouse pulls directly from PostgreSQL with no data flowing through our process. To get REPEATABLE READ, we'd need to stream data through our process (PG `COPY ... TO STDOUT` → our process → CH HTTP POST), which is a fundamentally different architecture with memory/throughput concerns for billion-row tables.
