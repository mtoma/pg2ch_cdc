#!/bin/bash
set -euo pipefail

# Regression test: migrating a table between timezones, and NOT thrashing it.
#
# This covers the two defects that made the 2026-09-07 repair attempt corrupt a
# production table (ciqfininstancedateunc, 4.7M rows, every value an offset out):
#
#   1. SPURIOUS DRIFT. `DESCRIBE TABLE postgresql()` always returns a bare
#      DateTime64(p); the table we build from it carries a timezone. Comparing
#      the two raw makes every pinned table look permanently drifted, so each
#      load dropped and recreated it — destroying the timezone and defeating
#      real drift detection.
#   2. STALE LOAD TIMEZONE. The loader used a timezone captured BEFORE that
#      recreate, so naive wall clocks were parsed in one zone and stored in a
#      column declaring another. Not just the DST-gap rows — EVERY row.
#
# So the load-time assertion here is deliberately "all rows match", not "the
# gap rows match": defect 2 leaves gap rows looking fine relative to each other
# while shifting the whole column.
#
# Expects: pg2ch_cdc built, PG and CH running.
# Environment: TEST_PG_HOST, TEST_PG_PORT, TEST_PG_USER, TEST_PG_PASSWORD,
#              TEST_PG_DATABASE, TEST_CH_HOST, TEST_CH_PORT, TEST_CH_USER,
#              TEST_CH_PASSWORD

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_tzmig"
MIRROR_NAME="test_tzmig"
CH_DATABASE="test_tzmig_ch"

export PGPASSWORD="$TEST_PG_PASSWORD"
PSQL="psql -h $TEST_PG_HOST -p $TEST_PG_PORT -U $TEST_PG_USER -d $TEST_PG_DATABASE -v ON_ERROR_STOP=1"
ch_query() {
    local response http_code
    response=$(curl -s -w "\n%{http_code}" "http://$TEST_CH_HOST:$TEST_CH_PORT" --data-binary "$1")
    http_code=$(echo "$response" | tail -1); response=$(echo "$response" | sed '$d')
    if [ "$http_code" -ge 400 ]; then echo "ClickHouse error ($http_code): $response" >&2; return 1; fi
    echo "$response"
}
fail() { echo "FAIL: $*" >&2; exit 1; }

write_config() {  # write_config <path> <timezone> [allow_dst]
    cat > "$1" <<EOF
mirror_name: $MIRROR_NAME
store_naive_timestamps_as_timezone: $2
timezone_allow_dst: ${3:-false}

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
  parallel_loads: 1
  binary: false
  ch_timeout_secs: 120

tables:
  - events
EOF
}

# Every row, compared against PostgreSQL. `toString(col)` with no timezone
# argument is the point: whatever zone the column is on, it must hand back the
# exact wall clock PostgreSQL holds.
compare_all() {
    local phase="$1" pg_out ch_out
    pg_out=$($PSQL -At -F$'\t' -c "
        SELECT id, coalesce(to_char(naive_ts,'YYYY-MM-DD HH24:MI:SS.US'),'~NULL~')
        FROM $SCHEMA.events ORDER BY id")
    ch_out=$(ch_query "
        SELECT id, ifNull(toString(naive_ts),'~NULL~')
        FROM $CH_DATABASE.events FINAL WHERE _pg2ch_is_deleted = 0
        ORDER BY id FORMAT TabSeparatedRaw")
    if [ "$pg_out" != "$ch_out" ]; then
        echo "--- diff (PG vs CH) ---"; diff <(echo "$pg_out") <(echo "$ch_out") | head -20 || true
        fail "$phase: ClickHouse does not match PostgreSQL"
    fi
    echo "  $phase: all rows match PostgreSQL"
}

# The table's full contents, for before/after stability checks. Used where the
# column is on a DST timezone and therefore CANNOT match PostgreSQL — there the
# question is whether a re-run left the table alone, not whether it is correct.
snapshot() {
    ch_query "SELECT id, ifNull(toString(naive_ts),'~NULL~')
              FROM $CH_DATABASE.events FINAL WHERE _pg2ch_is_deleted = 0
              ORDER BY id FORMAT TabSeparatedRaw"
}

declared_tz() {
    ch_query "SELECT type FROM system.columns WHERE database='$CH_DATABASE' AND table='events' AND name='naive_ts' FORMAT TabSeparatedRaw"
}

echo "=== Cleanup ==="
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME';" 2>/dev/null || true
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"

echo "=== PG table with values on both sides of a Europe/Paris spring-forward ==="
$PSQL <<'SQL'
CREATE SCHEMA test_tzmig;
CREATE TABLE test_tzmig.events (id INTEGER PRIMARY KEY, naive_ts TIMESTAMP);
INSERT INTO test_tzmig.events VALUES
    (1, '2019-03-31 02:01:39'),   -- inside the Paris gap: unstorable there
    (2, '2019-03-31 01:28:44'),   -- the hour the gap collapses onto
    (3, '2019-03-31 03:45:00'),   -- after the transition
    (4, '2020-06-15 12:34:56'),   -- ordinary summer (offset +02)
    (5, '2020-01-15 12:34:56'),   -- ordinary winter (offset +01)
    (6, NULL);
SQL

PARIS_CFG=$(mktemp /tmp/tzmig_paris_XXXXXX.yaml); write_config "$PARIS_CFG" "Europe/Paris" true
UTC_CFG=$(mktemp /tmp/tzmig_utc_XXXXXX.yaml);     write_config "$UTC_CFG"   "UTC"

echo "=== Phase 1: build the legacy state (Europe/Paris) ==="
"$BIN_DIR/pg2ch_cdc" --config "$PARIS_CFG" --plain >/dev/null
TZ1=$(declared_tz); echo "  naive_ts: $TZ1"
echo "$TZ1" | grep -q "Europe/Paris" || fail "phase 1 did not pin Europe/Paris: $TZ1"
GAP=$(ch_query "SELECT count() FROM $CH_DATABASE.events WHERE substring(toString(naive_ts),12,2)='02' SETTINGS final=0" | tr -d '[:space:]')
[ "$GAP" = "0" ] || fail "Europe/Paris should be unable to render 02:xx here, got $GAP"
echo "  confirmed: the gap row is lost under Europe/Paris (renders 0 rows at 02:xx)"

echo "=== Phase 2: re-running must NOT recreate the table (defect 1) ==="
# NOTE: no compare_all here. The column is still on Europe/Paris, so the gap row
# genuinely cannot match PostgreSQL — that is the defect under test, not a
# regression. What must hold is that a second run changes nothing at all.
BEFORE=$(snapshot)
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$PARIS_CFG" --plain 2>&1)
echo "$OUT" | grep -qi "Schema drift" && { echo "$OUT" | grep -i "schema drift"; fail "spurious schema drift on an unchanged table"; }
echo "$(declared_tz)" | grep -q "Europe/Paris" || fail "an unchanged run altered the timezone"
AFTER=$(snapshot)
[ "$BEFORE" = "$AFTER" ] || { diff <(echo "$BEFORE") <(echo "$AFTER") | head -10; fail "a no-op run altered the table contents"; }
echo "  phase 2: table untouched by a second run (no drift, same timezone, same rows)"

echo "=== Phase 3: migrate by dropping the table, with the config now on UTC ==="
ch_query "DROP TABLE $CH_DATABASE.events SYNC"
"$BIN_DIR/pg2ch_cdc" --config "$UTC_CFG" --plain >/dev/null
TZ2=$(declared_tz); echo "  naive_ts: $TZ2"
echo "$TZ2" | grep -q "'UTC'" || fail "migration did not pin UTC: $TZ2"

# The assertion that catches defect 2: EVERY row, not just the gap row.
compare_all "phase 3 (migrated to UTC)"
GAP=$(ch_query "SELECT count() FROM $CH_DATABASE.events WHERE substring(toString(naive_ts),12,2)='02' SETTINGS final=0" | tr -d '[:space:]')
[ "$GAP" = "1" ] || fail "expected the gap row to be storable under UTC, got $GAP rows at 02:xx"
VAL=$(ch_query "SELECT toString(naive_ts) FROM $CH_DATABASE.events WHERE id=1 FORMAT TabSeparatedRaw" | cut -c1-19)
[ "$VAL" = "2019-03-31 02:01:39" ] || fail "gap row is '$VAL', expected 2019-03-31 02:01:39"
echo "  the previously-unstorable value round-trips: $VAL"

echo "=== Phase 4: the migrated table must be stable and keep replicating ==="
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$UTC_CFG" --plain 2>&1)
echo "$OUT" | grep -qi "Schema drift" && { fail "spurious drift on the migrated table"; }
$PSQL -c "INSERT INTO $SCHEMA.events VALUES (7, '2021-03-28 02:30:00');" >/dev/null
"$BIN_DIR/pg2ch_cdc" --config "$UTC_CFG" --plain >/dev/null
compare_all "phase 4 (CDC into a migrated table)"
echo "$(declared_tz)" | grep -q "'UTC'" || fail "a CDC run altered the migrated timezone"

echo "=== Cleanup ==="
rm -f "$PARIS_CFG" "$UTC_CFG"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
echo "=== PASS ==="
