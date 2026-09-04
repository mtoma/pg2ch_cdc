#!/bin/bash
set -euo pipefail

# Integration test: timestamps survive a DST-observing ClickHouse server.
#
# This test only means anything if the ClickHouse server's own timezone
# observes DST (CI starts it with TZ=Europe/Paris). On a UTC server every
# assertion below passes even with the timezone handling removed, which is
# exactly why this class of bug went unnoticed for so long.
#
# What it pins down:
#   1. Naive PG `timestamp` values round-trip exactly, including the hour a
#      spring-forward skips and the hour an autumn fall-back repeats.
#   2. Both write paths agree — the postgresql() initial load and the CDC
#      TabSeparated insert.
#   3. `timestamptz` keeps its true instant.
#   4. Column types state their timezone.
#   5. A DST-observing `timezone:`, a missing one, and one that contradicts
#      an existing table are all refused rather than silently applied.
#
# Expects: pg2ch_cdc and pg2ch_diff built, PG and CH running.
# Environment: TEST_PG_HOST, TEST_PG_PORT, TEST_PG_USER, TEST_PG_PASSWORD,
#              TEST_PG_DATABASE, TEST_CH_HOST, TEST_CH_PORT, TEST_CH_USER,
#              TEST_CH_PASSWORD

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_tz"
MIRROR_NAME="test_tz"
CH_DATABASE="test_tz_ch"

export PGPASSWORD="$TEST_PG_PASSWORD"
PSQL="psql -h $TEST_PG_HOST -p $TEST_PG_PORT -U $TEST_PG_USER -d $TEST_PG_DATABASE -v ON_ERROR_STOP=1"
ch_query() {
    local response http_code
    response=$(curl -s -w "\n%{http_code}" "http://$TEST_CH_HOST:$TEST_CH_PORT" --data-binary "$1")
    http_code=$(echo "$response" | tail -1)
    response=$(echo "$response" | sed '$d')
    if [ "$http_code" -ge 400 ]; then
        echo "ClickHouse error ($http_code): $response" >&2
        return 1
    fi
    echo "$response"
}

write_config() {
    # write_config <path> <timezone>
    cat > "$1" <<EOF
mirror_name: $MIRROR_NAME
timezone: $2

source:
  host: $TEST_PG_HOST
  port: $TEST_PG_PORT
  database: $TEST_PG_DATABASE
  user: $TEST_PG_USER
  password: $TEST_PG_PASSWORD
  schema: $SCHEMA

destination:
  host: $TEST_CH_HOST
  port: $TEST_CH_PORT
  database: $CH_DATABASE
  user: $TEST_CH_USER
  password: $TEST_CH_PASSWORD

settings:
  batch_size: 100
  flush_interval_secs: 1
  parallel_loads: 2
  binary: ${BINARY_MODE:-false}
  ch_timeout_secs: 60

tables:
  - events
EOF
}

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "=== ClickHouse server timezone ==="
CH_SERVER_TZ=$(ch_query "SELECT serverTimeZone()" | tr -d '[:space:]')
CH_DST=$(ch_query "SELECT timeZoneOffset(toDateTime('2024-01-15 12:00:00','$CH_SERVER_TZ')) != timeZoneOffset(toDateTime('2024-07-15 12:00:00','$CH_SERVER_TZ'))" | tr -d '[:space:]')
echo "serverTimeZone() = $CH_SERVER_TZ (observes DST: $CH_DST)"
if [ "$CH_DST" != "1" ]; then
    echo "WARNING: ClickHouse server timezone '$CH_SERVER_TZ' does not observe DST."
    echo "         This test cannot detect the bug it exists to catch."
    echo "         Start ClickHouse with TZ=Europe/Paris for it to be meaningful."
fi

echo "=== Cleaning up from previous runs ==="
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"

echo "=== Creating PG schema with DST-boundary timestamps ==="
$PSQL <<'SQL'
CREATE SCHEMA test_tz;

CREATE TABLE test_tz.events (
    id          INTEGER PRIMARY KEY,
    label       TEXT NOT NULL,
    naive_ts    TIMESTAMP,          -- wall clock, no timezone
    tz_ts       TIMESTAMPTZ         -- an instant
);

-- Europe/Paris springs forward 02:00 -> 03:00 on the last Sunday of March.
-- The 02:xx values below do not exist in that timezone at all; the 01:xx
-- values do, and a lossy conversion collapses the two onto each other.
INSERT INTO test_tz.events (id, label, naive_ts, tz_ts) VALUES
    (1,  'spring gap 2020',        '2020-03-29 02:45:38', '2020-03-29 02:45:38+01'),
    (2,  'spring hour before',     '2020-03-29 01:45:38', '2020-03-29 01:45:38+01'),
    (3,  'spring hour after',      '2020-03-29 03:45:38', '2020-03-29 03:45:38+02'),
    (4,  'spring gap 2019',        '2019-03-31 02:24:10', '2019-03-31 02:24:10+01'),
    (5,  'spring gap 2025',        '2025-03-30 02:00:00', '2025-03-30 02:00:00+01'),
    (6,  'spring gap 2026',        '2026-03-29 02:59:59', '2026-03-29 02:59:59+01'),
    -- Autumn fall-back: 02:xx happens twice, so the instant is ambiguous.
    (7,  'autumn ambiguous 2020',  '2020-10-25 02:45:38', '2020-10-25 02:45:38+02'),
    (8,  'autumn ambiguous 2025',  '2025-10-26 02:30:00', '2025-10-26 02:30:00+02'),
    -- Ordinary values, and the shapes that hid the bug (midnight is never
    -- ambiguous because Paris transitions at 02:00).
    (9,  'midnight',               '2020-03-29 00:00:00', '2020-03-29 00:00:00+01'),
    (10, 'end of day',             '2020-06-15 23:59:59', '2020-06-15 23:59:59+02'),
    (11, 'microseconds',           '2021-07-04 12:34:56.123456', '2021-07-04 12:34:56.123456+02'),
    (12, 'pre-1970',               '1955-11-05 01:22:00', '1955-11-05 01:22:00+01'),
    (13, 'far future',             '2299-12-31 00:00:00', '2299-12-31 00:00:00+01'),
    (14, 'nulls',                  NULL, NULL);
SQL

echo "=== Running pg2ch_cdc (initial load path) with timezone: UTC ==="
MIRROR_CONFIG=$(mktemp /tmp/test_tz_XXXXXX.yaml)
write_config "$MIRROR_CONFIG" "UTC"
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Column types must state their timezone ==="
TYPES=$(ch_query "SELECT name, type FROM system.columns WHERE database='$CH_DATABASE' AND table='events' AND type LIKE '%DateTime%' ORDER BY name FORMAT TabSeparatedRaw")
echo "$TYPES"
echo "$TYPES" | grep -q "naive_ts.*DateTime64(6, 'UTC')" \
    || fail "naive_ts type does not state UTC: $TYPES"
echo "$TYPES" | grep -q "tz_ts.*DateTime64(6, 'UTC')" \
    || fail "tz_ts type does not state UTC: $TYPES"
echo "$TYPES" | grep -q "_pg2ch_synced_at.*DateTime64(9, 'UTC')" \
    || fail "_pg2ch_synced_at type does not state UTC: $TYPES"

# Compare PG and CH row for row.
#
# naive_ts is compared with NO timezone argument on the CH side: that is the
# point of the whole exercise — the column must hand back exactly the wall
# clock PostgreSQL holds, whatever timezone it is stored in.
#
# tz_ts is an instant, so it is compared as its UTC rendering. Both sides must
# format identically: CH toString() on a DateTime64(6) always emits 6 decimals,
# and PG's `.US` pattern zero-pads to 6 — an earlier version compared epoch
# seconds and failed only because one side trimmed the trailing zeros.
compare_all() {
    local phase="$1"
    local pg_out ch_out
    pg_out=$($PSQL -At -F$'\t' -c "
        SELECT id,
               coalesce(to_char(naive_ts,'YYYY-MM-DD HH24:MI:SS.US'),'~NULL~'),
               coalesce(to_char(tz_ts AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.US'),'~NULL~')
        FROM $SCHEMA.events ORDER BY id")
    ch_out=$(ch_query "
        SELECT id,
               ifNull(toString(naive_ts),'~NULL~'),
               ifNull(toString(tz_ts, 'UTC'),'~NULL~')
        FROM $CH_DATABASE.events FINAL WHERE _pg2ch_is_deleted = 0
        ORDER BY id FORMAT TabSeparatedRaw")

    if [ "$pg_out" != "$ch_out" ]; then
        echo "--- PG ---"; echo "$pg_out"
        echo "--- CH ---"; echo "$ch_out"
        echo "--- diff ---"; diff <(echo "$pg_out") <(echo "$ch_out") || true
        fail "$phase: ClickHouse does not match PostgreSQL"
    fi
    echo "$phase: all rows match PostgreSQL exactly"
}

echo "=== Verifying initial load ==="
compare_all "initial load"

echo "=== Verifying the spring-gap rows did not collapse onto each other ==="
# Rows 1 and 2 differ by exactly one hour in PG. A lossy conversion gives them
# the same instant, so this is the assertion that actually catches the bug.
COLLAPSE=$(ch_query "
    SELECT toUnixTimestamp64Micro(anyIf(naive_ts, id = 1))
         - toUnixTimestamp64Micro(anyIf(naive_ts, id = 2))
    FROM $CH_DATABASE.events FINAL WHERE _pg2ch_is_deleted = 0 AND id IN (1,2)" | tr -d '[:space:]')
[ "$COLLAPSE" = "3600000000" ] \
    || fail "spring-gap rows 1 and 2 are $COLLAPSE us apart, expected 3600000000 (they collapsed)"
echo "rows 1 and 2 remain 1h apart"

echo "=== Adding rows via CDC (TabSeparated insert path) ==="
$PSQL <<'SQL'
INSERT INTO test_tz.events (id, label, naive_ts, tz_ts) VALUES
    (20, 'cdc spring gap',     '2021-03-28 02:15:00', '2021-03-28 02:15:00+01'),
    (21, 'cdc hour before',    '2021-03-28 01:15:00', '2021-03-28 01:15:00+01'),
    (22, 'cdc autumn ambig',   '2021-10-31 02:15:00', '2021-10-31 02:15:00+02'),
    (23, 'cdc normal',         '2021-05-01 08:30:00', '2021-05-01 08:30:00+02'),
    (24, 'cdc nulls',          NULL, NULL);
UPDATE test_tz.events SET naive_ts = '2022-03-27 02:45:00', tz_ts = '2022-03-27 02:45:00+01' WHERE id = 9;
DELETE FROM test_tz.events WHERE id = 10;
SQL

"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Verifying CDC path ==="
compare_all "cdc"

echo "=== Verifying _pg2ch_synced_at is a truthful instant ==="
# It used to be written as a UTC wall clock into a server-timezone column,
# which put it an offset away from reality. Allow a generous window.
SYNC_SKEW=$(ch_query "
    SELECT abs(toInt64(toUnixTimestamp(max(_pg2ch_synced_at))) - toInt64(toUnixTimestamp(now())))
    FROM $CH_DATABASE.events SETTINGS final = 0" | tr -d '[:space:]')
[ "$SYNC_SKEW" -lt 600 ] \
    || fail "_pg2ch_synced_at is ${SYNC_SKEW}s from now() — it is on the wrong timezone convention"
echo "_pg2ch_synced_at is within ${SYNC_SKEW}s of now()"

echo "=== pg2ch_diff must agree ==="
DIFF_CONFIG=$(mktemp /tmp/test_tz_diff_XXXXXX.yaml)
cat > "$DIFF_CONFIG" <<EOF
mirror_name: $MIRROR_NAME
timezone: UTC

source:
  host: $TEST_PG_HOST
  port: $TEST_PG_PORT
  database: $TEST_PG_DATABASE
  user: $TEST_PG_USER
  password: $TEST_PG_PASSWORD
  schema: $SCHEMA

destination:
  host: $TEST_CH_HOST
  port: $TEST_CH_PORT
  database: $CH_DATABASE
  user: $TEST_CH_USER
  password: $TEST_CH_PASSWORD

tables:
  - name: events
    level: checksum
EOF
DIFF_OUTPUT=$("$BIN_DIR/pg2ch_diff" --config "$DIFF_CONFIG" --plain)
echo "$DIFF_OUTPUT"
echo "$DIFF_OUTPUT" | grep -q "0 mismatches" || fail "pg2ch_diff reported mismatches"

echo "=== A DST-observing timezone must be refused ==="
BAD_CONFIG=$(mktemp /tmp/test_tz_bad_XXXXXX.yaml)
write_config "$BAD_CONFIG" "Europe/Paris"
if OUT=$("$BIN_DIR/pg2ch_cdc" --config "$BAD_CONFIG" --plain 2>&1); then
    fail "timezone: Europe/Paris was accepted; it observes DST and must be refused"
fi
echo "$OUT" | grep -qi "daylight saving" || fail "DST rejection message unclear: $OUT"
echo "$OUT" | grep -q "timezone_allow_dst: true" || fail "DST rejection does not mention the opt-in: $OUT"
echo "refused, and the message names the reason and the opt-in"

echo "=== A DST timezone WITH timezone_allow_dst must be accepted, loudly ==="
# The escape hatch exists for mirrors already holding DST-convention data.
# It must work, and it must warn. Uses a throwaway CH database so the
# UTC-stored table above is left alone.
ALLOW_CONFIG=$(mktemp /tmp/test_tz_allow_XXXXXX.yaml)
CH_DATABASE_SAVED="$CH_DATABASE"
CH_DATABASE="${CH_DATABASE}_dst"
write_config "$ALLOW_CONFIG" "Europe/Paris"
printf 'timezone_allow_dst: true\n' >> "$ALLOW_CONFIG"
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$ALLOW_CONFIG" --plain 2>&1) \
    || { echo "$OUT"; fail "timezone_allow_dst: true did not allow Europe/Paris"; }
echo "$OUT" | grep -qi "timezone_allow_dst: true" \
    || fail "no warning printed for an allowed DST timezone: $OUT"
echo "$OUT" | grep -qi "cannot be represented" \
    || fail "DST warning does not name the defect: $OUT"
# And the columns must be pinned to the DST timezone, not left bare.
ALLOW_TYPES=$(ch_query "SELECT type FROM system.columns WHERE database='$CH_DATABASE' AND table='events' AND name='naive_ts' FORMAT TabSeparatedRaw")
echo "$ALLOW_TYPES" | grep -q "Europe/Paris" \
    || fail "naive_ts not pinned to Europe/Paris: $ALLOW_TYPES"
echo "accepted, warned, and pinned to Europe/Paris"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
CH_DATABASE="$CH_DATABASE_SAVED"
rm -f "$ALLOW_CONFIG"

echo "=== An unknown timezone must be refused ==="
write_config "$BAD_CONFIG" "Not/AZone"
if OUT=$("$BIN_DIR/pg2ch_cdc" --config "$BAD_CONFIG" --plain 2>&1); then
    fail "timezone: Not/AZone was accepted"
fi
echo "$OUT" | grep -qi "does not recognise timezone" || fail "unknown-timezone message unclear: $OUT"
echo "refused"

echo "=== A missing timezone must be refused ==="
grep -v '^timezone:' "$MIRROR_CONFIG" > "$BAD_CONFIG"
if OUT=$("$BIN_DIR/pg2ch_cdc" --config "$BAD_CONFIG" --plain 2>&1); then
    fail "a config with no timezone: was accepted"
fi
echo "$OUT" | grep -q "timezone: UTC" || fail "missing-timezone message does not say what to add: $OUT"
echo "refused, and the message says what to add"

echo "=== A config timezone that disagrees with the table must NOT override it ==="
# The table is stored on UTC. Pointing a config at a different (DST-free)
# timezone must neither be refused nor silently applied: the column type is the
# authority, so the run adopts UTC and leaves the data and the type alone.
# Overriding here would read every value an offset away from what was written,
# and refusing would force an all-or-nothing migration of the whole mirror.
write_config "$BAD_CONFIG" "Asia/Kolkata"
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$BAD_CONFIG" --plain 2>&1) \
    || { echo "$OUT"; fail "a config timezone differing from the table stopped the run"; }
echo "$OUT" | grep -q "stores timestamps in 'UTC'" \
    || fail "run did not report adopting the table's own timezone: $OUT"
STILL_UTC=$(ch_query "SELECT type FROM system.columns WHERE database='$CH_DATABASE' AND table='events' AND name='naive_ts' FORMAT TabSeparatedRaw")
echo "$STILL_UTC" | grep -q "'UTC'" \
    || fail "the config overwrote the table's timezone: $STILL_UTC"
compare_all "with a disagreeing config timezone"
echo "adopted the table's UTC, left the type and the data untouched"

echo "=== Re-running with the original timezone is still idempotent ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain
compare_all "after re-run"

echo "=== A table on a DIFFERENT timezone from the config must be adopted, not refused ==="
# This is what an incremental, table-by-table migration looks like midway
# through. The column type is the authority; the config only supplies the
# default for new tables.
#
# Note the two column kinds migrate differently:
#   naive_ts (from PG `timestamp`) is a WALL CLOCK  -> shift the instant so the
#           reading is preserved, then restate the type.
#   tz_ts    (from PG `timestamptz`) is an INSTANT  -> restate the type only;
#           shifting it would corrupt a value that is already correct.
ch_query "ALTER TABLE $CH_DATABASE.events UPDATE naive_ts = toDateTime64(toString(naive_ts,'UTC'), 6, 'Etc/GMT-1') WHERE 1"
for i in $(seq 1 60); do
    DONE=$(ch_query "SELECT count() FROM system.mutations WHERE database='$CH_DATABASE' AND table='events' AND NOT is_done" | tr -d '[:space:]')
    [ "$DONE" = "0" ] && break
    sleep 1
done
ch_query "ALTER TABLE $CH_DATABASE.events MODIFY COLUMN naive_ts Nullable(DateTime64(6, 'Etc/GMT-1')), MODIFY COLUMN tz_ts Nullable(DateTime64(6, 'Etc/GMT-1'))"
MIG_TYPES=$(ch_query "SELECT name, type FROM system.columns WHERE database='$CH_DATABASE' AND table='events' AND name IN ('naive_ts','tz_ts') FORMAT TabSeparatedRaw")
echo "$MIG_TYPES" | grep -q "Etc/GMT-1" || fail "migration did not restate the types: $MIG_TYPES"

# The config still says UTC. The run must succeed and adopt Etc/GMT-1.
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain 2>&1) \
    || { echo "$OUT"; fail "a table on a non-config timezone was refused"; }
echo "$OUT" | grep -q "stores timestamps in 'Etc/GMT-1'" \
    || fail "run did not report adopting the table's own timezone: $OUT"
compare_all "after migrating one table to Etc/GMT-1"
echo "adopted the table's timezone, values preserved"

echo "=== CDC must keep writing that table correctly (parses per column) ==="
# The client-wide session_timezone is still UTC, but a declared column parses
# against its own timezone — so these must land exactly, gap value included.
$PSQL <<'SQL'
INSERT INTO test_tz.events (id, label, naive_ts, tz_ts) VALUES
    (30, 'post-migration gap',    '2024-03-31 02:20:00', '2024-03-31 02:20:00+01'),
    (31, 'post-migration normal', '2024-08-01 09:15:00', '2024-08-01 09:15:00+02'),
    (32, 'post-migration null',   NULL, NULL);
SQL
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain
compare_all "cdc into a migrated table"
MIGRATED_TZ=$(ch_query "SELECT type FROM system.columns WHERE database='$CH_DATABASE' AND table='events' AND name='naive_ts' FORMAT TabSeparatedRaw")
echo "$MIGRATED_TZ" | grep -q "Etc/GMT-1" || fail "CDC run changed the table's timezone: $MIGRATED_TZ"
echo "CDC wrote a non-config-timezone table correctly and left its type alone"

echo "=== A table whose columns disagree must warn, not stop ==="
# Only that table's initial load is impossible; CDC is unaffected.
ch_query "ALTER TABLE $CH_DATABASE.events MODIFY COLUMN tz_ts Nullable(DateTime64(6, 'UTC'))"
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain 2>&1) \
    || { echo "$OUT"; fail "a table with mixed column timezones stopped the run"; }
echo "$OUT" | grep -qi "mixes timezones" || fail "no warning for mixed column timezones: $OUT"
echo "warned and carried on"
# Put it back so the table is internally consistent again.
ch_query "ALTER TABLE $CH_DATABASE.events MODIFY COLUMN tz_ts Nullable(DateTime64(6, 'Etc/GMT-1'))"

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG" "$DIFF_CONFIG" "$BAD_CONFIG"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
