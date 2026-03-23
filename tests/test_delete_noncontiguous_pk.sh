#!/bin/bash
set -euo pipefail

# Regression test: DELETE on a table where PK columns are non-contiguous.
#
# Reproduces the bug found on fds.sanc_v1_sanc_ent_affiliates:
#   PK = (col1, col2, col4) with col3 being a non-PK column.
#   DELETEs send only PK columns in the K tuple, but build_delete_row()
#   must map them back to the correct column positions. If the mapping
#   is wrong, the delete marker gets a wrong PK value (e.g. 0 instead of
#   the real value) and ReplacingMergeTree treats it as a different row.
#
# Table layout (mirrors fds.sanc_v1_sanc_ent_affiliates):
#   entity_id       TEXT     PK col 1
#   affiliated_id   TEXT     PK col 2
#   pct_held        DOUBLE   non-PK (sits between PK cols!)
#   aff_type_code   INTEGER  PK col 3
#
# The test:
#   1. Initial load of 5 rows
#   2. DELETE 2 rows in PG
#   3. Run CDC
#   4. Verify CH has exactly 3 rows (not 5)

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_del_ncpk"
MIRROR_NAME="test_del_ncpk"
CH_DATABASE="test_del_ncpk_ch"

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

echo "=== Cleaning up from previous runs ==="
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;" 2>/dev/null || true
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"

echo "=== Creating PG table with non-contiguous PK ==="
$PSQL <<'SQL'
CREATE SCHEMA test_del_ncpk;

-- PK is (entity_id, affiliated_id, aff_type_code) — columns 1, 2, 4
-- pct_held (column 3) is NOT part of the PK
CREATE TABLE test_del_ncpk.affiliates (
    entity_id       TEXT NOT NULL,
    affiliated_id   TEXT NOT NULL,
    pct_held        DOUBLE PRECISION,
    aff_type_code   INTEGER NOT NULL,
    PRIMARY KEY (entity_id, affiliated_id, aff_type_code)
);

INSERT INTO test_del_ncpk.affiliates VALUES
    ('ENT-A', 'AFF-1', 50.0,  2),
    ('ENT-B', 'AFF-1', 75.5,  3),
    ('ENT-C', 'AFF-2', NULL,  2),
    ('ENT-D', 'AFF-3', 100.0, 2),
    ('ENT-E', 'AFF-1', 25.0,  3);
SQL

echo "=== Verifying PG data ==="
PG_COUNT=$($PSQL -t -c "SELECT count(*) FROM test_del_ncpk.affiliates;" | tr -d ' ')
echo "PG: $PG_COUNT rows"
[ "$PG_COUNT" = "5" ] || { echo "FAIL: expected 5 rows, got $PG_COUNT"; exit 1; }

echo "=== Writing mirror config ==="
MIRROR_CONFIG=$(mktemp /tmp/test_del_ncpk_XXXXXX.yaml)
cat > "$MIRROR_CONFIG" <<EOF
mirror_name: $MIRROR_NAME

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
  ch_timeout_secs: 60

tables:
  - affiliates
EOF

echo "=== Running pg2ch_cdc (initial load) ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Verifying initial load in CH ==="
CH_COUNT=$(ch_query "SELECT count() FROM $CH_DATABASE.affiliates FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "CH after initial load: $CH_COUNT rows"
[ "$CH_COUNT" = "5" ] || { echo "FAIL: expected 5 rows after initial load, got $CH_COUNT"; exit 1; }

echo "=== Deleting 2 rows from PG ==="
$PSQL <<'SQL'
-- Delete rows where aff_type_code is part of the PK and non-zero
-- These DELETEs send K tuple with 3 values: entity_id, affiliated_id, aff_type_code
DELETE FROM test_del_ncpk.affiliates WHERE entity_id = 'ENT-A' AND affiliated_id = 'AFF-1' AND aff_type_code = 2;
DELETE FROM test_del_ncpk.affiliates WHERE entity_id = 'ENT-B' AND affiliated_id = 'AFF-1' AND aff_type_code = 3;
SQL

PG_AFTER=$($PSQL -t -c "SELECT count(*) FROM test_del_ncpk.affiliates;" | tr -d ' ')
echo "PG after deletes: $PG_AFTER rows"
[ "$PG_AFTER" = "3" ] || { echo "FAIL: expected 3 rows after delete, got $PG_AFTER"; exit 1; }

echo "=== Running pg2ch_cdc (CDC pass) ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Verifying deletes replicated to CH ==="
CH_AFTER=$(ch_query "SELECT count() FROM $CH_DATABASE.affiliates FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "CH after CDC: $CH_AFTER rows (expected 3)"

# Also check for orphaned rows — rows in CH that don't exist in PG
CH_TOTAL_RAW=$(ch_query "SELECT count() FROM $CH_DATABASE.affiliates SETTINGS final=0" | tr -d '[:space:]')
CH_DELETED=$(ch_query "SELECT count() FROM $CH_DATABASE.affiliates FINAL WHERE _pg2ch_is_deleted = 1" | tr -d '[:space:]')
echo "CH raw rows: $CH_TOTAL_RAW, deleted markers: $CH_DELETED"

# Dump all rows for debugging if test fails
if [ "$CH_AFTER" != "3" ]; then
    echo "=== DEBUG: All CH rows (no FINAL) ==="
    ch_query "SELECT entity_id, affiliated_id, aff_type_code, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.affiliates ORDER BY entity_id, _pg2ch_version SETTINGS final=0"
    echo "=== DEBUG: CH rows with FINAL ==="
    ch_query "SELECT entity_id, affiliated_id, aff_type_code, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.affiliates FINAL ORDER BY entity_id"
    echo "FAIL: expected 3 rows after CDC deletes, got $CH_AFTER"
    rm -f "$MIRROR_CONFIG"
    exit 1
fi

echo "=== Running diff to double-check ==="
DIFF_CONFIG=$(mktemp /tmp/test_del_ncpk_diff_XXXXXX.yaml)
cat > "$DIFF_CONFIG" <<EOF
mirror_name: $MIRROR_NAME

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
  - name: affiliates
    level: exact_count
EOF

DIFF_OUTPUT=$("$BIN_DIR/pg2ch_diff" --config "$DIFF_CONFIG" --plain)
echo "$DIFF_OUTPUT"
echo "$DIFF_OUTPUT" | grep -q "0 mismatches" || { echo "FAIL: diff found mismatches"; rm -f "$MIRROR_CONFIG" "$DIFF_CONFIG"; exit 1; }

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG" "$DIFF_CONFIG"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;" 2>/dev/null || true
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
