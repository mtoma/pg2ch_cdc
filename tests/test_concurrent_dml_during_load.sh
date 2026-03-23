#!/bin/bash
set -euo pipefail

# Test: DML on table B while table A is still loading.
#
# Scenario:
#   1. Table A is large (~500K rows), table B is small (5 rows)
#   2. pg2ch_cdc snapshots LSN, starts loading A (takes seconds)
#   3. While A loads, we INSERT/DELETE/INSERT/DELETE rows in B on PG side
#   4. Table B loads AFTER those changes (postgresql() snapshot is newer)
#   5. CDC replays from pre-load LSN, re-applying changes B already has
#   6. ReplacingMergeTree must resolve overlaps correctly
#
# The INSERT+DELETE+INSERT+DELETE pattern is the most dangerous because:
#   - Some rows exist at snapshot time, get deleted, get re-inserted
#   - Some rows don't exist at snapshot time, get inserted, get deleted
#   - Table B's load sees the final state, but CDC replays the full history

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

-- Table A: large enough that postgresql() load takes a few seconds
CREATE TABLE test_concurrent.big_table (
    id INTEGER PRIMARY KEY,
    payload TEXT NOT NULL
);
INSERT INTO test_concurrent.big_table
    SELECT i, repeat('x', 200)
    FROM generate_series(1, 500000) AS i;

-- Table B: small table where concurrent DML will happen
-- Non-contiguous PK: (entity_id, category_code) with value in between
CREATE TABLE test_concurrent.active_table (
    entity_id   TEXT NOT NULL,
    value       DOUBLE PRECISION,
    category    INTEGER NOT NULL,
    PRIMARY KEY (entity_id, category)
);

-- Initial rows in B (these exist BEFORE the snapshot LSN)
INSERT INTO test_concurrent.active_table VALUES
    ('E1', 10.0, 1),
    ('E2', 20.0, 2),
    ('E3', 30.0, 3),
    ('E4', 40.0, 1),
    ('E5', 50.0, 2);
SQL

echo "=== Verifying PG data ==="
BIG_COUNT=$($PSQL -t -c "SELECT count(*) FROM test_concurrent.big_table;" | tr -d ' ')
SMALL_COUNT=$($PSQL -t -c "SELECT count(*) FROM test_concurrent.active_table;" | tr -d ' ')
echo "PG: big_table=$BIG_COUNT, active_table=$SMALL_COUNT"

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
  ch_timeout_secs: 120

tables:
  - big_table
  - active_table
EOF

# ── DML script: runs in background while pg2ch_cdc loads table A ──
DML_SCRIPT=$(mktemp /tmp/test_concurrent_dml_XXXXXX.sh)
cat > "$DML_SCRIPT" <<'DMLEOF'
#!/bin/bash
export PGPASSWORD="$1"
PSQL="psql -h $2 -p $3 -U $4 -d $5 -v ON_ERROR_STOP=1"

# Wait a moment for pg2ch_cdc to snapshot LSN and start loading big_table
sleep 1

echo "DML: Starting concurrent modifications on active_table..."

# Round 1: DELETE existing rows, INSERT new ones
$PSQL -c "DELETE FROM test_concurrent.active_table WHERE entity_id = 'E1' AND category = 1;"
$PSQL -c "DELETE FROM test_concurrent.active_table WHERE entity_id = 'E2' AND category = 2;"
$PSQL -c "INSERT INTO test_concurrent.active_table VALUES ('E6', 60.0, 3);"
$PSQL -c "INSERT INTO test_concurrent.active_table VALUES ('E7', 70.0, 1);"

# Round 2: DELETE the rows we just inserted, re-insert deleted rows with new values
$PSQL -c "DELETE FROM test_concurrent.active_table WHERE entity_id = 'E6' AND category = 3;"
$PSQL -c "DELETE FROM test_concurrent.active_table WHERE entity_id = 'E7' AND category = 1;"
$PSQL -c "INSERT INTO test_concurrent.active_table VALUES ('E1', 11.0, 1);"
$PSQL -c "INSERT INTO test_concurrent.active_table VALUES ('E2', 22.0, 2);"

# Round 3: More churn — delete and re-insert with different values
$PSQL -c "DELETE FROM test_concurrent.active_table WHERE entity_id = 'E3' AND category = 3;"
$PSQL -c "INSERT INTO test_concurrent.active_table VALUES ('E3', 33.0, 3);"
$PSQL -c "INSERT INTO test_concurrent.active_table VALUES ('E8', 80.0, 2);"
$PSQL -c "DELETE FROM test_concurrent.active_table WHERE entity_id = 'E8' AND category = 2;"

echo "DML: Done. Final state should be: E1(11,1), E2(22,2), E3(33,3), E4(40,1), E5(50,2)"
DMLEOF
chmod +x "$DML_SCRIPT"

echo "=== Running pg2ch_cdc with concurrent DML ==="
# Start DML script in background — it will modify active_table while big_table loads
bash "$DML_SCRIPT" "$TEST_PG_PASSWORD" "$TEST_PG_HOST" "$TEST_PG_PORT" "$TEST_PG_USER" "$TEST_PG_DATABASE" &
DML_PID=$!

# Run pg2ch_cdc (loads big_table first since parallel_loads=1, then active_table)
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

# Wait for DML to finish (should already be done)
wait $DML_PID 2>/dev/null || true

echo "=== Verifying PG final state ==="
PG_FINAL=$($PSQL -t -c "SELECT count(*) FROM test_concurrent.active_table;" | tr -d ' ')
echo "PG active_table: $PG_FINAL rows"
[ "$PG_FINAL" = "5" ] || { echo "FAIL: expected 5 rows in PG, got $PG_FINAL"; exit 1; }

echo "=== Verifying CH final state ==="
CH_BIG=$(ch_query "SELECT count() FROM $CH_DATABASE.big_table FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
CH_ACTIVE=$(ch_query "SELECT count() FROM $CH_DATABASE.active_table FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "CH: big_table=$CH_BIG, active_table=$CH_ACTIVE"

[ "$CH_BIG" = "500000" ] || { echo "FAIL: expected 500000 in big_table, got $CH_BIG"; exit 1; }

if [ "$CH_ACTIVE" != "$PG_FINAL" ]; then
    echo "=== DEBUG: PG rows ==="
    $PSQL -c "SELECT * FROM test_concurrent.active_table ORDER BY entity_id, category;"
    echo "=== DEBUG: CH rows (no FINAL) ==="
    ch_query "SELECT entity_id, value, category, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.active_table ORDER BY entity_id, category, _pg2ch_version SETTINGS final=0"
    echo "=== DEBUG: CH rows (FINAL) ==="
    ch_query "SELECT entity_id, value, category, _pg2ch_version, _pg2ch_is_deleted FROM $CH_DATABASE.active_table FINAL ORDER BY entity_id, category"
    echo "FAIL: CH active_table has $CH_ACTIVE rows, PG has $PG_FINAL"
    rm -f "$MIRROR_CONFIG" "$DML_SCRIPT"
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
  - name: big_table
    level: exact_count
  - name: active_table
    level: exact_count
EOF

DIFF_OUTPUT=$("$BIN_DIR/pg2ch_diff" --config "$DIFF_CONFIG" --plain)
echo "$DIFF_OUTPUT"
echo "$DIFF_OUTPUT" | grep -q "0 mismatches" || { echo "FAIL: diff found mismatches"; rm -f "$MIRROR_CONFIG" "$DIFF_CONFIG" "$DML_SCRIPT"; exit 1; }

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG" "$DIFF_CONFIG" "$DML_SCRIPT"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;" 2>/dev/null || true
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
