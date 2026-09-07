#!/bin/bash
set -euo pipefail

# Regression test: a slot must advance through WAL that holds nothing for it.
#
# This is the failure that took down the source database on 2026-09-04. Three
# mirrors shared one busy PostgreSQL. A ~850 GB burst landed on tables belonging
# to one publication; the other slots had no changes of their own to decode, so
# they could not confirm anything, so PostgreSQL could not recycle any of it.
# 2.1 TB of WAL accumulated and the disk filled.
#
# The shape reproduced here is the same, in miniature:
#   - `mirrored` is in the publication and gets ONE row.
#   - `unrelated` is NOT in the publication and gets a large volume of churn.
#
# A client that only confirms its own decoded changes leaves the slot pinned
# behind all of `unrelated`'s WAL. A client that also confirms the `walEnd` of
# primary keepalives walks straight through it.
#
# Expects: pg2ch_cdc built, PG and CH running.
# Environment: TEST_PG_HOST, TEST_PG_PORT, TEST_PG_USER, TEST_PG_PASSWORD,
#              TEST_PG_DATABASE, TEST_CH_HOST, TEST_CH_PORT, TEST_CH_USER,
#              TEST_CH_PASSWORD

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_slot"
MIRROR_NAME="test_slot"
CH_DATABASE="test_slot_ch"

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

echo "=== Creating a mirrored table and an unrelated, un-mirrored one ==="
$PSQL <<'SQL'
CREATE SCHEMA test_slot;
CREATE TABLE test_slot.mirrored (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE test_slot.unrelated (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL);
INSERT INTO test_slot.mirrored (id, label) VALUES (1, 'only row');
SQL

MIRROR_CONFIG=$(mktemp /tmp/test_slot_XXXXXX.yaml)
cat > "$MIRROR_CONFIG" <<EOF
mirror_name: $MIRROR_NAME
store_naive_timestamps_as_timezone: UTC

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
  ch_timeout_secs: 60

tables:
  - mirrored
EOF

echo "=== Establishing the slot (initial load + first drain) ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
BASE=$(pgq "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME'")
echo "confirmed_flush_lsn after setup: $BASE"
[ -n "$BASE" ] || fail "slot pg2ch_$MIRROR_NAME was not created"

echo "=== Generating WAL that belongs to NOBODY's publication ==="
# Enough to span multiple WAL segments so there is real ground to cover.
$PSQL <<'SQL'
INSERT INTO test_slot.unrelated (payload)
SELECT repeat(md5(g::text), 40) FROM generate_series(1, 400000) g;
SQL
$PSQL -c "CHECKPOINT;" >/dev/null 2>&1 || true

PENDING=$(pgq "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME'")
PENDING_H=$(pgq "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME'")
echo "WAL now pending for the slot: $PENDING_H ($PENDING bytes)"
[ "$PENDING" -gt 8000000 ] || fail "test did not generate enough unrelated WAL ($PENDING bytes) to be meaningful"

echo "=== Draining: the publication has NO new changes at all ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain 2>&1 | tail -5

AFTER=$(pgq "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME'")
REMAIN=$(pgq "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name='pg2ch_$MIRROR_NAME'")
ADVANCED=$(pgq "SELECT pg_wal_lsn_diff('$AFTER'::pg_lsn, '$BASE'::pg_lsn)::bigint")
echo "confirmed_flush_lsn: $BASE -> $AFTER  (advanced $ADVANCED bytes, $REMAIN still pending)"

# The assertion. Without keepalive-driven advancement this is 0 and the slot
# pins every byte of unrelated WAL indefinitely.
[ "$ADVANCED" -gt 8000000 ] \
    || fail "slot advanced only $ADVANCED bytes through $PENDING bytes of unrelated WAL — it is pinning WAL it has no interest in"
echo "slot walked through the unrelated WAL"

echo "=== And the mirrored table must be untouched and still correct ==="
CH_ROWS=$(ch_query "SELECT count() FROM $CH_DATABASE.mirrored FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$CH_ROWS" = "1" ] || fail "expected 1 mirrored row, got $CH_ROWS"

echo "=== A real change after all that WAL must still replicate ==="
# Proves advancement did not skip the publication's own data.
$PSQL -c "INSERT INTO test_slot.mirrored (id, label) VALUES (2, 'after the burst');" >/dev/null
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
CH_ROWS=$(ch_query "SELECT count() FROM $CH_DATABASE.mirrored FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$CH_ROWS" = "2" ] || fail "post-burst change did not replicate: expected 2 rows, got $CH_ROWS"
LABEL=$(ch_query "SELECT label FROM $CH_DATABASE.mirrored FINAL WHERE id = 2 AND _pg2ch_is_deleted = 0" | tr -d '\n')
[ "$LABEL" = "after the burst" ] || fail "wrong value replicated: '$LABEL'"
echo "post-burst change replicated correctly"

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
