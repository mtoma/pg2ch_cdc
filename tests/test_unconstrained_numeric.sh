#!/bin/bash
set -euo pipefail

# Regression test: a PostgreSQL `numeric` with no declared precision must load.
#
# This is the failure that stopped the cstat mirror on 2026-09-14. PG's
# unconstrained `numeric` is arbitrary-precision; ClickHouse's postgresql()
# reader maps it to Decimal(38, 19) — 19 digits before the point — so a value
# >= 10^19 cannot be read at all:
#
#   Code: 69. DB::Exception: Decimal value is too big: 20 digits were read:
#   '17485804441876462000'. Expected to read decimal with scale 19 and
#   precision 38: While executing PostgreSQL.
#
# It fails on the READ, so widening the destination column does not help. And
# because it aborts the batch it takes the whole run down, not just the row:
# one row of 2,382,042 blocked a 149-table mirror for 3h31m and left the
# replication slot 74 GB behind.
#
# The fix is to stop letting ClickHouse infer the type. The load reads through
# `CREATE TABLE ... ENGINE = PostgreSQL` with a structure we declare, and
# src/typemap.rs maps an unconstrained numeric to Decimal(76, 19) — 57 digits
# before the point.
#
# Also covers the `column_types:` override, which is the escape hatch for a
# column the default mapping should not be bent to accommodate.
#
# Expects: pg2ch_cdc built, PG and CH running.
# Environment: TEST_PG_HOST, TEST_PG_PORT, TEST_PG_USER, TEST_PG_PASSWORD,
#              TEST_PG_DATABASE, TEST_CH_HOST, TEST_CH_PORT, TEST_CH_USER,
#              TEST_CH_PASSWORD

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_numeric"
MIRROR_NAME="test_numeric"
CH_DATABASE="test_numeric_ch"

# The actual value from cstat.sec_dtrt that stopped the mirror.
BIG_VALUE="17485804441876462000"
# What that column normally looks like, for contrast.
NORMAL_VALUE="1.09883623"
# Scale beyond DLTools.py's DECIMAL(50,10) fallback, which would truncate it.
PRECISE_VALUE="0.1234567890123456"

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
pgq() { $PSQL -At -c "$1"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

echo "=== Cleaning up from previous runs ==="
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"

echo "=== A table whose numeric columns declare no precision ==="
$PSQL <<SQL
CREATE SCHEMA $SCHEMA;
CREATE TABLE $SCHEMA.rates (
    id       INTEGER PRIMARY KEY,
    label    TEXT NOT NULL,
    factor   NUMERIC,          -- unconstrained: the shape that broke cstat
    declared NUMERIC(18, 4),   -- declared: must be preserved exactly
    dbl      DOUBLE PRECISION  -- must become Float64, never Decimal
);
INSERT INTO $SCHEMA.rates VALUES
    (1, 'normal',  $NORMAL_VALUE,  12.3456, 1.5),
    (2, 'precise', $PRECISE_VALUE, 0.0001,  2.5),
    (3, 'huge',    $BIG_VALUE,     99.9999, 3.5),
    (4, 'null',    NULL,           NULL,    NULL);
SQL
echo "  PG holds: $(pgq "SELECT factor FROM $SCHEMA.rates WHERE id = 3")"

MIRROR_CONFIG=$(mktemp /tmp/test_numeric_XXXXXX.yaml)
cat > "$MIRROR_CONFIG" <<EOF
mirror_name: $MIRROR_NAME
store_naive_timestamps_as_timezone: UTC

# The override exists for exceptions the default mapping should not be bent
# for. Here it pins a declared numeric to a wider type than it needs, purely
# to prove the override reaches the CREATE, the load and the drift check.
column_types:
  rates:
    declared: Nullable(Decimal(38, 4))

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
  batch_size: 1000
  flush_interval_secs: 1
  parallel_loads: 1
  binary: false
  ch_timeout_secs: 120

tables:
  - rates
EOF

echo "=== Initial load (this is what used to die with Code: 69) ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== The chosen column types ==="
TYPES=$(ch_query "SELECT name, type FROM system.columns WHERE database='$CH_DATABASE' AND table='rates' AND name NOT LIKE '_pg2ch_%' ORDER BY position FORMAT TabSeparatedRaw")
echo "$TYPES" | sed 's/^/  /'

check_type() {
    local col="$1" want="$2"
    local got
    got=$(echo "$TYPES" | awk -F'\t' -v c="$col" '$1==c{print $2}')
    [ "$got" = "$want" ] || fail "column $col: expected '$want', got '$got'"
}
# Unconstrained numeric must get room: 76-19 = 57 digits before the point.
check_type factor   "Nullable(Decimal(76, 19))"
# A declared numeric would be Decimal(18, 4); the override must win.
check_type declared "Nullable(Decimal(38, 4))"
# A binary float is not a decimal. DLTools.py maps this to DECIMAL; we do not.
check_type dbl      "Nullable(Float64)"
# PK is never Nullable even though ClickHouse would allow it.
check_type id       "Int32"

echo "=== Every value survived the load ==="
ROWS=$(ch_query "SELECT count() FROM $CH_DATABASE.rates FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$ROWS" = "4" ] || fail "expected 4 rows after load, got $ROWS"

GOT=$(ch_query "SELECT toString(factor) FROM $CH_DATABASE.rates FINAL WHERE id = 3" | tr -d '[:space:]')
[ "${GOT%%.*}" = "$BIG_VALUE" ] || fail "the 20-digit value did not survive: got '$GOT', want '$BIG_VALUE'"
echo "  factor(id=3) = $GOT"

# DLTools.py's DECIMAL(50,10) fallback would truncate this to 10 places.
GOT=$(ch_query "SELECT toString(factor) FROM $CH_DATABASE.rates FINAL WHERE id = 2" | tr -d '[:space:]')
case "$GOT" in
    "$PRECISE_VALUE"*) echo "  factor(id=2) = $GOT (scale preserved)" ;;
    *) fail "scale was truncated: got '$GOT', want '$PRECISE_VALUE...'" ;;
esac

NULLS=$(ch_query "SELECT count() FROM $CH_DATABASE.rates FINAL WHERE id = 4 AND factor IS NULL AND dbl IS NULL" | tr -d '[:space:]')
[ "$NULLS" = "1" ] || fail "NULLs did not survive the load"

echo "=== The load source must not be left behind ==="
LEFT=$(ch_query "SELECT count() FROM system.tables WHERE database='$CH_DATABASE' AND name LIKE '__pg2ch_src_%'" | tr -d '[:space:]')
[ "$LEFT" = "0" ] || fail "$LEFT load-source proxy table(s) left behind in $CH_DATABASE"

echo "=== CDC must carry an oversized value too, not just the load ==="
$PSQL -c "INSERT INTO $SCHEMA.rates VALUES (5, 'huge via cdc', 98765432109876543210, 1.0, 9.5);"
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
GOT=$(ch_query "SELECT toString(factor) FROM $CH_DATABASE.rates FINAL WHERE id = 5" | tr -d '[:space:]')
[ "${GOT%%.*}" = "98765432109876543210" ] || fail "CDC lost the oversized value: got '$GOT'"
echo "  factor(id=5) = $GOT"

echo "=== A second run must be a no-op, not a drop-and-recreate ==="
# If the override or the mapping were missing from the drift comparison, the
# table would read as drifted forever and be silently recreated every run.
OUT=$("$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain 2>&1)
if echo "$OUT" | grep -q "Schema drift"; then
    echo "$OUT" | grep "Schema drift" | sed 's/^/  /'
    fail "stable schema reported as drift — the drift check disagrees with the CREATE"
fi
ROWS=$(ch_query "SELECT count() FROM $CH_DATABASE.rates FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$ROWS" = "5" ] || fail "row count changed on a no-op run: $ROWS"

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
