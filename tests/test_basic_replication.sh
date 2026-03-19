#!/bin/bash
set -euo pipefail

# Integration test: create 2 PG tables, replicate to CH, validate with diff.
#
# Expects: pg2ch_cdc and pg2ch_diff binaries built, PG and CH running.
# Environment: TEST_PG_HOST, TEST_PG_PORT, TEST_PG_USER, TEST_PG_PASSWORD,
#              TEST_PG_DATABASE, TEST_CH_HOST, TEST_CH_PORT, TEST_CH_USER,
#              TEST_CH_PASSWORD

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_repl"
MIRROR_NAME="test_repl"
CH_DATABASE="test_repl_ch"

export PGPASSWORD="$TEST_PG_PASSWORD"
PSQL="psql -h $TEST_PG_HOST -p $TEST_PG_PORT -U $TEST_PG_USER -d $TEST_PG_DATABASE -v ON_ERROR_STOP=1"
CH="curl -sf http://$TEST_CH_HOST:$TEST_CH_PORT"

echo "=== Cleaning up from previous runs ==="
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$CH --data-binary "DROP DATABASE IF EXISTS $CH_DATABASE"

echo "=== Creating PG schema and tables ==="
$PSQL <<'SQL'
CREATE SCHEMA test_repl;

CREATE TABLE test_repl.users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
);

CREATE TABLE test_repl.orders (
    order_id INTEGER NOT NULL,
    line_num INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, line_num)
);

INSERT INTO test_repl.users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com'),
    (4, 'Diana', 'diana@example.com'),
    (5, 'Eve', 'eve@example.com');

INSERT INTO test_repl.orders (order_id, line_num, user_id, amount) VALUES
    (100, 1, 1, 29.99),
    (100, 2, 1, 14.50),
    (101, 1, 2, 99.00),
    (102, 1, 3, 7.25),
    (102, 2, 3, 12.75),
    (102, 3, 3, 45.00),
    (103, 1, 4, 199.99),
    (104, 1, 5, 5.50),
    (104, 2, 5, 8.25),
    (105, 1, 1, 33.33);
SQL

echo "=== Verifying PG data ==="
USER_COUNT=$($PSQL -t -c "SELECT count(*) FROM test_repl.users;" | tr -d ' ')
ORDER_COUNT=$($PSQL -t -c "SELECT count(*) FROM test_repl.orders;" | tr -d ' ')
echo "PG: $USER_COUNT users, $ORDER_COUNT orders"
[ "$USER_COUNT" = "5" ] || { echo "FAIL: expected 5 users, got $USER_COUNT"; exit 1; }
[ "$ORDER_COUNT" = "10" ] || { echo "FAIL: expected 10 orders, got $ORDER_COUNT"; exit 1; }

echo "=== Writing mirror config ==="
MIRROR_CONFIG=$(mktemp /tmp/test_mirror_XXXXXX.yaml)
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
  parallel_loads: 2
  binary: false
  ch_timeout_secs: 60

tables:
  - users
  - orders
EOF

echo "=== Running pg2ch_cdc ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain

echo "=== Verifying CH data ==="
CH_USERS=$($CH --data-binary "SELECT count() FROM $CH_DATABASE.users FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
CH_ORDERS=$($CH --data-binary "SELECT count() FROM $CH_DATABASE.orders FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
echo "CH: $CH_USERS users, $CH_ORDERS orders"
[ "$CH_USERS" = "5" ] || { echo "FAIL: expected 5 users in CH, got $CH_USERS"; exit 1; }
[ "$CH_ORDERS" = "10" ] || { echo "FAIL: expected 10 orders in CH, got $CH_ORDERS"; exit 1; }

echo "=== Writing diff config ==="
DIFF_CONFIG=$(mktemp /tmp/test_diff_XXXXXX.yaml)
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
  - name: users
    level: exact_count
  - name: orders
    level: exact_count
EOF

echo "=== Running pg2ch_diff ==="
DIFF_OUTPUT=$("$BIN_DIR/pg2ch_diff" --config "$DIFF_CONFIG" --plain)
echo "$DIFF_OUTPUT"

echo "$DIFF_OUTPUT" | grep -q "0 mismatches" || { echo "FAIL: diff found mismatches"; exit 1; }

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG" "$DIFF_CONFIG"
$CH --data-binary "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
