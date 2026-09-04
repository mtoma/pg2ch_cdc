# Timezones

`timezone:` is mandatory in every mirror and diff config. There is no default,
and adding one would reintroduce the bug the setting exists to prevent.

It sets the timezone for **newly created** tables and for pinning columns that
never declared one. For a table that already declares a timezone, **the column
type wins** — see "The column type is the authority" below. That is deliberate:
it lets a mirror be migrated one table at a time.

## The type mismatch this resolves

| PostgreSQL | What it is | Carries a timezone? |
|---|---|---|
| `timestamp` | a wall clock | no |
| `timestamptz` | an instant | yes (rendered with an offset) |

ClickHouse has one type for both: `DateTime64(p[, tz])` — always an **instant**
plus a **display timezone**. There is no naive datetime type. So storing a
PostgreSQL `timestamp` requires choosing a timezone to read it in, and
something has to make that choice explicit.

## Why the ClickHouse server default is not an acceptable answer

`DESCRIBE TABLE postgresql(...)` returns a bare `DateTime64(6)` — never a
timezone, and no setting changes that. A column created from it inherits the
server's `timezone` setting, and three properties make that a trap:

1. **It is invisible from the config.** Nothing in the mirror file records
   which convention the data is on.
2. **It is frozen at table creation, and readers cannot override it.**
   `session_timezone` does *not* apply to a table column — ClickHouse's own
   docs note that a value parsed from a string "inherits the type and time zone
   of the existing column", so a consumer cannot ask for the column in UTC.
3. **It differs between hosts.** Restoring onto a differently-configured
   server, or changing the setting, silently reinterprets every stored row.

So pg2ch_cdc requires the timezone in the config and writes it into the column
type. The convention then travels with the data and is visible in
`system.columns`.

## Why DST timezones are rejected

`validate_timezone_in_ch` refuses any timezone whose January and July offsets
differ. In such a zone the mapping between wall clocks and instants is neither
total nor injective:

- **Spring forward** (e.g. Europe/Paris, 2020-03-29, `02:00 → 03:00`): the hour
  `02:00–02:59` does not exist. A PostgreSQL value inside it has no
  representable instant, and ClickHouse resolves it to the hour *before* the
  gap — where genuine `01:xx` data already lives. Two distinct source values
  become one stored value, and nothing in the row records which it was. This is
  data loss, not a rounding error, and it is not recoverable from ClickHouse
  alone.
- **Fall back** (2020-10-25, `03:00 → 02:00`): `02:00–02:59` happens twice, so
  two instants render identically. The wall clock survives a round trip, but
  the stored instant is arbitrary between the two — silently wrong by an hour
  for anyone treating the value as a true instant.

Fixed-offset zones (`UTC`, `Asia/Kolkata`, `Etc/GMT+5`) have neither problem.
**UTC is the right answer unless local wall clocks are a hard requirement.**

### The `timezone_allow_dst` escape hatch

A mirror that already holds data on a DST timezone is in a bind: the stored
convention is unrepresentable, but refusing to start blocks replication
entirely until a full migration completes. `timezone_allow_dst: true` accepts
the defect and continues, warning on every run.

It is deliberately awkward: no default, has to be written into the config, and
prints a ten-line warning each run. Do not set it for a new mirror — there is
no reason to choose a lossy convention from scratch.

## The column type is the authority

The two write paths resolve naive timestamps differently, and the difference
decides what a mixed-convention mirror can do. Measured on ClickHouse 26.4,
inserting the naive string `2020-06-15 12:00:00` into three columns in one
request:

| `session_timezone` | `DateTime64(6,'Europe/Paris')` | `DateTime64(6,'UTC')` | `DateTime64(6)` |
|---|---|---|---|
| Europe/Paris | 1592215200 | 1592222400 | 1592215200 |
| UTC | 1592215200 | 1592222400 | **1592222400** |

**A TabSeparated insert is column-aware.** A column that declares a timezone
parses against *that* timezone and ignores `session_timezone` entirely; only a
bare column follows the session. So CDC writes every column correctly whatever
convention each is on, with no coordination — even two different conventions in
one table, in one insert.

**The `postgresql()` initial load is not.** It resolves the naive value once per
request and merely copies the resulting instant into the column, so only the
column whose timezone matches `session_timezone` reads back correctly. Same PG
value `1989-06-21 00:00:00`, loaded into the same three columns:

| `session_timezone` | Paris col | UTC col | bare col |
|---|---|---|---|
| Europe/Paris | `00:00:00` ✓ | `1989-06-20 22:00:00` ✗ | `00:00:00` ✓ |
| UTC | `02:00:00` ✗ | `00:00:00` ✓ | `02:00:00` ✗ |

Hence the design:

- `resolve_table_timezone` reads each existing table's convention from its
  column types and **adopts** it. A table on a different timezone from the
  config is normal, not an error — it is what an incremental migration looks
  like halfway through.
- The load `INSERT` carries `SETTINGS session_timezone = '<that table's tz>'`,
  which overrides the client-wide default. CDC needs no such handling.
- A table whose own DateTime columns disagree gets a **warning**, not a stop:
  its initial load is impossible, but its CDC is fine. Migrate all of a table's
  DateTime columns together and this never arises.

## How the pieces fit

- **Column types** — `clickhouse::pin_datetime_timezone` writes the configured
  timezone into whatever DateTime type ClickHouse chose, at `CREATE TABLE`.
  It never decides a type; it only qualifies one. Unit-tested in
  `clickhouse.rs`.
- **Every ClickHouse request** carries `session_timezone=<tz>` and
  `date_time_input_format=best_effort` (`ChClient::settings`). The first makes
  naive timestamps resolve against the config rather than the server; the second
  makes ClickHouse honour the `+01`-style offset on a `timestamptz`.
- **`types.rs` does no timezone arithmetic.** Timestamp text is forwarded
  verbatim, in both text and binary mode (binary `timestamptz` gets an explicit
  `+00:00`, since it decodes to UTC). ClickHouse resolves everything.
- **`_pg2ch_synced_at` is not written from Rust.** The column's
  `DEFAULT now64()` fills it server-side. Sending a wall clock computed in Rust
  (`chrono::Utc::now()` formatted naively) put the audit column on a different
  convention from the data columns next to it — it held a UTC wall clock in a
  server-timezone column, an offset away from the truth.

  Note for existing mirrors: rows already in `_pg2ch_synced_at` keep the old,
  offset value — pinning does not move stored instants — so on a non-UTC server
  that column ends up holding old wrong values and new correct ones. It is an
  audit column that nothing joins on, so this is left alone deliberately rather
  than rewritten; the data columns are what matter. If you want it uniform,
  rewrite it with the same two-step `ALTER` as any other column.
- **A table whose timezone differs from the config warns.** Adopting the table's
  own timezone is right for a migration in progress, but from inside the process
  a deliberate migration and a typo'd `timezone:` look identical — and since the
  disagreement no longer stops the run, silence would let a wrong config create
  new tables on a second convention unnoticed. The warning names both timezones
  and says which one new tables would get.
- **Existing tables** go through `resolve_table_timezone`. It adopts whatever
  the columns declare and pins any column that declares nothing (metadata-only —
  ClickHouse rewrites no data for a timezone-only `MODIFY COLUMN`). A table that
  predates pinning is pinned to the **server default**, because that is what its
  stored instants actually mean; if that differs from the config it says so and
  explains how to migrate.

## Migrating an existing mirror

No full reload, and no config change. Migrate **one table at a time**; the next
run adopts each table as you go.

Set `timezone:` to the server default first, so every column gets pinned to what
it actually holds. Then, per table, migrate all of its DateTime columns together:

```sql
-- 1. shift the instants so the new reading equals the old one.
--    ONLY for columns from a PG `timestamp` (a wall clock).
ALTER TABLE db.tbl UPDATE col = toDateTime64(toString(col, '<old_tz>'), 6, 'UTC') WHERE 1;

-- 2. restate the types (metadata-only, and the only step a `timestamptz`
--    column needs — its instant is already correct, only its display moves).
ALTER TABLE db.tbl MODIFY COLUMN col Nullable(DateTime64(6, 'UTC'));
```

Between the two statements that column reads wrong, so run them as a pair. Wait
for `system.mutations` to drain before step 2.

Two things to know before pricing it:

- **A mutation rewrites only the columns it changes**; the rest are hardlinked.
  So the cost is the size of those columns, not of the table.
- **A column in the sorting key cannot be `UPDATE`d** (`Cannot UPDATE key
  column`, code 420) — `MODIFY` still works, but shifting its values requires
  rebuilding the table (`INSERT INTO new SELECT …`, then `RENAME`).

Values that fell in a spring-forward gap under the old timezone were already
lost, and step 1 faithfully preserves that error — those rows have to be re-read
from PostgreSQL by primary key. Narrow the candidate set in ClickHouse first
(see the detection query above); it is a few tens of thousands of rows, so the
repair is index lookups, not a scan.

## Testing

`tests/test_timezone_dst.sh` covers both write paths, both DST boundaries,
`timestamptz`, the pinned types, and every rejection. **It only means anything
on a DST-observing ClickHouse server** — CI starts ClickHouse with
`TZ=Europe/Paris` and asserts `serverTimeZone()`. On a UTC server the whole
class of bug is invisible.
