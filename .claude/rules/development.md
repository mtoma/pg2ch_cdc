# pg2ch_cdc — Development Rules

## Philosophy

This tool is an **rsync for PostgreSQL → ClickHouse**. Every design decision flows from that metaphor:
- Run once, do what's needed, exit
- Determine state from source and destination — never from state files
- Idempotent: safe to run on every cron tick, safe to kill mid-run
- Explicit: print what you see and what you'll do before doing it

## Code style

- **Keep it SQL**: The Rust code is a thin orchestrator around SQL queries. Use `pg.query()` / `pg.execute()` / `ch.query()` with plain SQL strings. No ORMs, no query builders, no abstraction layers over SQL.
- **Synchronous only**: No tokio, no async. Single-threaded, simple `libc::poll()` loop for CDC.
- **Minimal Rust API surface**: pg.rs has exactly two methods (`execute`, `query`). Don't add more.
- **Dynamic columns**: ClickHouse HTTP interface with TabSeparated format. No compile-time Row structs (tables have varying schemas).

## Architecture constraints

- **libpq FFI** for PostgreSQL (replication protocol requires it)
- **reqwest::blocking** for ClickHouse HTTP interface
- **ReplacingMergeTree** with `_pg2ch_version` and `_pg2ch_is_deleted` columns on every target table
- **pgoutput** logical decoding plugin (built into PG 10+)
- Type mapping via `DESCRIBE TABLE postgresql()` — ClickHouse performs the PG→CH type conversion itself, so we never maintain a manual mapping

## When modifying code

- Don't add daemon/long-running mode. The tool runs, syncs, exits.
- Don't add state files or persistent tracking. Everything is determined from live data.
- Don't add async/tokio. The synchronous model is a deliberate choice.
- Don't add abstractions for hypothetical future needs. Keep it concrete.
- Don't change the `_pg2ch_*` column naming convention (synced_at, is_deleted, version).
- Don't change the publication/slot naming convention: `pg2ch_{mirror_name}`.
- Test against the real servers (see CLAUDE.md for hostnames) — there's no test infrastructure.

## Building

```bash
export PATH="$HOME/.cargo/bin:$PATH"
BINDGEN_EXTRA_CLANG_ARGS="-I/usr/include/pgsql" cargo build --release
```

## Known limitations

- Single process per mirror config (no parallel CDC — parallel initial loads are supported)
- No periodic health checks beyond the post-CDC integrity check
- REPEATABLE READ mode not available (see `initial-load-consistency.md` for rationale)
