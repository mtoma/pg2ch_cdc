# Timezones

`timezone:` is mandatory in every mirror and diff config. There is no default,
and adding one would reintroduce the bug the setting exists to prevent.

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
- **Existing tables** are checked by `ensure_timezone_pinned`. Where the stored
  timezone matches the config it pins the type (metadata-only — ClickHouse
  rewrites no data for a timezone-only `MODIFY COLUMN`). Where it differs, or
  where an unpinned column sits on a server default that contradicts the config,
  it bails with the `ALTER` statements needed to migrate. It never writes two
  conventions into one column.

## Migrating an existing mirror

For a mirror already running on the server default, set `timezone:` to that
same value first. pg2ch_cdc will pin every column, which changes nothing
observable and makes the data immune to a later server-config change. Only
then consider moving to UTC, per column:

```sql
-- 1. shift the instants so the UTC reading equals the old reading
ALTER TABLE db.tbl UPDATE col = toDateTime64(toString(col, '<old_tz>'), 6, 'UTC') WHERE 1;
-- 2. restate the type (metadata-only)
ALTER TABLE db.tbl MODIFY COLUMN col Nullable(DateTime64(6, 'UTC'));
```

Run the pair per table: between the two statements the column reads wrong.
Values that fell in a spring-forward gap under the old timezone were already
lost and step 1 preserves the error — those rows must be re-read from
PostgreSQL by primary key.

## Testing

`tests/test_timezone_dst.sh` covers both write paths, both DST boundaries,
`timestamptz`, the pinned types, and every rejection. **It only means anything
on a DST-observing ClickHouse server** — CI starts ClickHouse with
`TZ=Europe/Paris` and asserts `serverTimeZone()`. On a UTC server the whole
class of bug is invisible.
