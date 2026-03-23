#!/bin/bash
set -euo pipefail

# Test: reload of table B while WAL contains pending DML for it.
#
# Simulates deterministically what happens when table A takes hours to load
# and DML changes accumulate on table B during that time:
#
#   Step 1: Initial load of both tables (clean baseline)
#   Step 2: DML on table B in PG (INSERT/DELETE/INSERT/DELETE churn)
#   Step 3: Truncate table B in CH (forces reload on next run)
#   Step 4: Run pg2ch_cdc again — it will:
#           - Reload B via postgresql() (sees post-DML final state, version=0)
#           - CDC replays from OLD slot position (replays same DML on top)
#           - ReplacingMergeTree must resolve the overlap correctly
#
# This is exactly what happens in production when:
#   - LSN is snapshotted at T0
#   - Table A loads from T0 to T0+6h
#   - DML on B happens at T0+1h, T0+2h, etc.
#   - Table B loads at T0+6h (sees final state)
#   - CDC replays T0→T0+6h (replays all DML that B already has)
#
# No timing dependency — fully deterministic.

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_concurrent"
MIRROR_NAME="test_concurrent"
CH_DATABASE="test_concurrent_ch"

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
CREATE SCHEMA test_concurrent;

CREATE TABLE test_concurrent.table_a (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO test_concurrent.table_a VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie');

-- Non-contiguous PK: (entity_id, category) with value column in between
CREATE TABLE test_concurrent.table_b (
    entity_id   TEXT NOT NULL,
    value       DOUBLE PRECISION,
    category    INTEGER NOT NULL,
    PRIMARY KEY (entity_id, category)
);
INSERT INTO test_concurrent.table_b VALUES
    ('E1', 10.0, 1),
    ('E2', 20.0, 2),
    ('E3', 30.0, 3),
    ('E4', 40.0, 1),
    ('E5', 50.0, 2);
SQL

echo "=== Writing mirror config ==="
MIRROR_CONFIG=$(mktemp /tmp/test_concurrent_XXXXXX.yaml)
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
  - table_a
  - table_b
EOF

echo "=== Step 1: Initial load (clean baseline) ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

CH_A=$(ch_query "SELECT count() FROM $CH_DATABASE.table_a FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
CH_B=$(ch_query "SELECT count() FROM $CH_DATABASE.table_b FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "After initial load: table_a=$CH_A, table_b=$CH_B"
[ "$CH_A" = "3" ] || { echo "FAIL: expected 3 in table_a, got $CH_A"; exit 1; }
[ "$CH_B" = "5" ] || { echo "FAIL: expected 5 in table_b, got $CH_B"; exit 1; }

echo "=== Step 2: DML churn on table_b in PG ==="
$PSQL <<'SQL'
-- Round 1: delete existing rows, insert new ones
DELETE FROM test_concurrent.table_b WHERE entity_id = 'E1' AND category = 1;
DELETE FROM test_concurrent.table_b WHERE entity_id = 'E2' AND category = 2;
INSERT INTO test_concurrent.table_b VALUES ('E6', 60.0, 3);
INSERT INTO test_concurrent.table_b VALUES ('E7', 70.0, 1);

-- Round 2: delete just-inserted rows, re-insert previously deleted rows
DELETE FROM test_concurrent.table_b WHERE entity_id = 'E6' AND category = 3;
DELETE FROM test_concurrent.table_b WHERE entity_id = 'E7' AND category = 1;
INSERT INTO test_concurrent.table_b VALUES ('E1', 11.0, 1);
INSERT INTO test_concurrent.table_b VALUES ('E2', 22.0, 2);

-- Round 3: more churn — update-via-delete+insert, insert+delete
DELETE FROM test_concurrent.table_b WHERE entity_id = 'E3' AND category = 3;
INSERT INTO test_concurrent.table_b VALUES ('E3', 33.0, 3);
INSERT INTO test_concurrent.table_b VALUES ('E8', 80.0, 2);
DELETE FROM test_concurrent.table_b WHERE entity_id = 'E8' AND category = 2;
SQL

PG_B=$($PSQL -t -c "SELECT count(*) FROM test_concurrent.table_b;" | tr -d ' ')
echo "PG table_b after DML: $PG_B rows"
[ "$PG_B" = "5" ] || { echo "FAIL: expected 5 rows in PG, got $PG_B"; exit 1; }

echo "=== Step 3: Drop table_b in CH (simulate need for reload) ==="
ch_query "DROP TABLE $CH_DATABASE.table_b SYNC"

echo "=== Step 4: Run pg2ch_cdc (reload B + CDC replay of same DML) ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Verifying final state ==="
CH_A_FINAL=$(ch_query "SELECT count() FROM $CH_DATABASE.table_a FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
CH_B_FINAL=$(ch_query "SELECT count() FROM $CH_DATABASE.table_b FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
PG_B_FINAL=$($PSQL -t -c "SELECT count(*) FROM test_concurrent.table_b;" | tr -d ' ')
echo "CH: table_a=$CH_A_FINAL, table_b=$CH_B_FINAL  PG: table_b=$PG_B_FINAL"

if [ "$CH_B_FINAL" != "$PG_B_FINAL" ]; then
    echo "=== DEBUG: PG rows ==="
    $PSQL -c "SELECT * FROM test_concurrent.table_b ORDER BY entity_id, category;"
    echo "=== DEBUG: CH rows (no FINAL) ==="
    ch_query "SELECT entity_id, value, category, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.table_b ORDER BY entity_id, category, _pg2ch_version SETTINGS final=0"
    echo "=== DEBUG: CH rows (FINAL, including deleted) ==="
    ch_query "SELECT entity_id, value, category, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.table_b FINAL ORDER BY entity_id, category"
    echo "FAIL: CH table_b has $CH_B_FINAL rows, PG has $PG_B_FINAL"
    rm -f "$MIRROR_CONFIG"
    exit 1
fi

echo "=== Running diff to double-check ==="
DIFF_CONFIG=$(mktemp /tmp/test_concurrent_diff_XXXXXX.yaml)
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
  - name: table_a
    level: exact_count
  - name: table_b
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
