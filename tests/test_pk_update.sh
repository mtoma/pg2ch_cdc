#!/bin/bash
set -euo pipefail

# Test: UPDATE that changes primary key columns.
#
# When a PK column is updated, the old PK identity must be deleted in CH
# and the new PK identity inserted. Otherwise the old row becomes a phantom
# that lives forever in ReplacingMergeTree.
#
# With REPLICA IDENTITY DEFAULT, pgoutput sends the old key as a 'K' tuple
# in the UPDATE message when PK columns change. pg2ch_cdc must:
#   1. Write a delete marker for the old PK (is_deleted=1)
#   2. Write the new row with the new PK (is_deleted=0)
#
# This test covers:
#   - Simple PK update (single-column PK)
#   - Composite PK update (change one PK column, keep the other)
#   - Non-contiguous composite PK update
#   - Multiple PK changes on the same row

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_pk_update"
MIRROR_NAME="test_pk_update"
CH_DATABASE="test_pk_update_ch"

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

echo "=== Creating PG tables ==="
$PSQL <<'SQL'
CREATE SCHEMA test_pk_update;

-- Simple single-column PK
CREATE TABLE test_pk_update.simple (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO test_pk_update.simple VALUES
    (1, 'alpha'),
    (2, 'bravo'),
    (3, 'charlie');

-- Composite PK with non-PK column in between (like sanc_v1_sanc_ent_affiliates)
CREATE TABLE test_pk_update.composite (
    entity_id   TEXT NOT NULL,
    value       DOUBLE PRECISION,
    category    INTEGER NOT NULL,
    PRIMARY KEY (entity_id, category)
);
INSERT INTO test_pk_update.composite VALUES
    ('E1', 10.0, 1),
    ('E2', 20.0, 2),
    ('E3', 30.0, 3);
SQL

echo "=== Writing mirror config ==="
MIRROR_CONFIG=$(mktemp /tmp/test_pk_update_XXXXXX.yaml)
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
  - simple
  - composite
EOF

echo "=== Step 1: Initial load ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

CH_SIMPLE=$(ch_query "SELECT count() FROM $CH_DATABASE.simple FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
CH_COMP=$(ch_query "SELECT count() FROM $CH_DATABASE.composite FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "After initial load: simple=$CH_SIMPLE, composite=$CH_COMP"
[ "$CH_SIMPLE" = "3" ] || { echo "FAIL: expected 3 in simple"; exit 1; }
[ "$CH_COMP" = "3" ] || { echo "FAIL: expected 3 in composite"; exit 1; }

echo "=== Step 2: PK-changing UPDATEs in PG ==="
$PSQL <<'SQL'
-- Simple table: change PK from 1 to 10 (old PK 1 must become phantom if not handled)
UPDATE test_pk_update.simple SET id = 10 WHERE id = 1;

-- Simple table: change PK from 2 to 20, also change name
UPDATE test_pk_update.simple SET id = 20, name = 'bravo-updated' WHERE id = 2;

-- Composite table: change the non-contiguous PK column (category)
-- Old PK (E1, 1) must be deleted, new PK (E1, 99) must be inserted
UPDATE test_pk_update.composite SET category = 99 WHERE entity_id = 'E1' AND category = 1;

-- Composite table: change the first PK column
-- Old PK (E2, 2) → new PK (E2-NEW, 2)
UPDATE test_pk_update.composite SET entity_id = 'E2-NEW' WHERE entity_id = 'E2' AND category = 2;

-- Composite table: change both PK columns at once
-- Old PK (E3, 3) → new PK (E3-NEW, 33)
UPDATE test_pk_update.composite SET entity_id = 'E3-NEW', category = 33 WHERE entity_id = 'E3' AND category = 3;
SQL

echo "=== Verifying PG state after PK updates ==="
echo "simple:"
$PSQL -c "SELECT * FROM test_pk_update.simple ORDER BY id;"
echo "composite:"
$PSQL -c "SELECT * FROM test_pk_update.composite ORDER BY entity_id, category;"

PG_SIMPLE=$($PSQL -t -c "SELECT count(*) FROM test_pk_update.simple;" | tr -d ' ')
PG_COMP=$($PSQL -t -c "SELECT count(*) FROM test_pk_update.composite;" | tr -d ' ')
echo "PG counts: simple=$PG_SIMPLE, composite=$PG_COMP"

echo "=== Step 3: Run CDC ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Verifying CH state ==="
CH_SIMPLE_AFTER=$(ch_query "SELECT count() FROM $CH_DATABASE.simple FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
CH_COMP_AFTER=$(ch_query "SELECT count() FROM $CH_DATABASE.composite FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "CH counts: simple=$CH_SIMPLE_AFTER, composite=$CH_COMP_AFTER"

FAIL=0

if [ "$CH_SIMPLE_AFTER" != "$PG_SIMPLE" ]; then
    echo "=== DEBUG: simple — CH rows (no FINAL) ==="
    ch_query "SELECT id, name, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.simple ORDER BY id, _pg2ch_version SETTINGS final=0"
    echo "=== DEBUG: simple — CH rows (FINAL) ==="
    ch_query "SELECT id, name, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.simple FINAL ORDER BY id"
    echo "FAIL: simple has $CH_SIMPLE_AFTER rows in CH, expected $PG_SIMPLE"
    FAIL=1
fi

if [ "$CH_COMP_AFTER" != "$PG_COMP" ]; then
    echo "=== DEBUG: composite — CH rows (no FINAL) ==="
    ch_query "SELECT entity_id, value, category, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.composite ORDER BY entity_id, category, _pg2ch_version SETTINGS final=0"
    echo "=== DEBUG: composite — CH rows (FINAL) ==="
    ch_query "SELECT entity_id, value, category, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.composite FINAL ORDER BY entity_id, category"
    echo "FAIL: composite has $CH_COMP_AFTER rows in CH, expected $PG_COMP"
    FAIL=1
fi

if [ "$FAIL" = "1" ]; then
    rm -f "$MIRROR_CONFIG"
    exit 1
fi

# Verify no phantom old PKs remain
echo "=== Checking for phantom old PKs ==="
PHANTOM_SIMPLE=$(ch_query "SELECT count() FROM $CH_DATABASE.simple FINAL WHERE _pg2ch_is_deleted = 0 AND id IN (1, 2)" | tr -d '[:space:]')
PHANTOM_COMP=$(ch_query "SELECT count() FROM $CH_DATABASE.composite FINAL WHERE _pg2ch_is_deleted = 0 AND (entity_id, category) IN (('E1', 1), ('E2', 2), ('E3', 3))" | tr -d '[:space:]')
echo "Phantom old PKs: simple=$PHANTOM_SIMPLE (expect 0), composite=$PHANTOM_COMP (expect 0)"
[ "$PHANTOM_SIMPLE" = "0" ] || { echo "FAIL: phantom rows in simple (old PKs 1,2 still alive)"; rm -f "$MIRROR_CONFIG"; exit 1; }
[ "$PHANTOM_COMP" = "0" ] || { echo "FAIL: phantom rows in composite (old PKs still alive)"; rm -f "$MIRROR_CONFIG"; exit 1; }

echo "=== Running diff ==="
DIFF_CONFIG=$(mktemp /tmp/test_pk_update_diff_XXXXXX.yaml)
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
  - name: simple
    level: exact_count
  - name: composite
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
