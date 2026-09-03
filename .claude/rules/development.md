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
- Don't do timezone arithmetic in Rust. ClickHouse owns it — we set
  `session_timezone` and forward timestamp text verbatim. Doing it here would
  mean carrying a timezone database and keeping it in exact agreement with the
  `postgresql()` initial-load path, which runs inside ClickHouse.
- Don't give `timezone:` a default. An invisible default is the bug this
  setting exists to prevent.
- Don't make the config override a table's existing timezone. The column type is
  the authority; the config is only the default for new tables. Overriding it
  would force an all-or-nothing migration of the whole mirror.
- Don't change the publication/slot naming convention: `pg2ch_{mirror_name}`.
- Test against the real servers (see CLAUDE.md for hostnames), and run
  `tests/*.sh` against containers. **ClickHouse must run on a DST-observing
  timezone** (CI uses `TZ=Europe/Paris`) — on a UTC server every timezone bug
  is invisible.

## Building

Requires a Rust toolchain, clang/libclang (for bindgen), and the PostgreSQL
client development headers.

bindgen must be pointed at the directory containing `libpq-fe.h`, and that
path varies by platform. If `pg_config` is on PATH it will tell you:

```bash
BINDGEN_EXTRA_CLANG_ARGS="-I$(pg_config --includedir)" cargo build --release
```

`pg_config` is not shipped by every distribution's libpq package, so fall back
to the conventional location:

| Platform | `libpq-fe.h` location | Dev package |
| --- | --- | --- |
| Debian, Ubuntu | `/usr/include/postgresql` | `libpq-dev`, `libclang-dev` |
| RHEL, Fedora | `/usr/include/pgsql` | `libpq-devel`, `clang-devel` |
| openSUSE | `/usr/include/pgsql` | `postgresql-devel`, `clang-devel` |
| Arch | `/usr/include` | `postgresql-libs`, `clang` |
| macOS (Homebrew) | `$(brew --prefix libpq)/include` | `libpq`, `llvm` |

```bash
BINDGEN_EXTRA_CLANG_ARGS="-I/usr/include/postgresql" cargo build --release
```

If bindgen fails with `'libpq-fe.h' file not found`, the include path is
wrong — locate the header (`find /usr/include -name libpq-fe.h`) and pass its
directory.

## Deploying

Build on the machine you deploy to, or on one whose glibc is no newer. The
binary links dynamically against the build host's libc, so a build from a
rolling-release workstation will fail to load on a stable-release server with
an older glibc. Pull the source and rebuild in place rather than copying a
binary between machines.

Rebuilding while an invocation is in flight is safe: cargo renames a freshly
written file into place, so a running process keeps executing the old inode
until it exits and the next invocation picks up the new binary. No need to
pause the scheduler.

## Known limitations

- Single process per mirror config (no parallel CDC — parallel initial loads are supported)
- No periodic health checks beyond the post-CDC integrity check
- REPEATABLE READ mode not available (see `initial-load-consistency.md` for rationale)
