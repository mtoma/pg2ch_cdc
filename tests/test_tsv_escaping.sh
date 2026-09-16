#!/bin/bash
set -euo pipefail

# Guard on the CDC serialisation path: values that collide with TabSeparated's
# own syntax must round-trip byte for byte.
#
# Every row the mirror receives is turned into TabSeparated text by
# CdcBatch/tsv_escape_into, so a tab, a newline or a backslash inside a value
# is indistinguishable from a field separator, a row separator or an escape
# unless it is escaped. The literal two-character string \N is the nastiest
# case: it is ClickHouse's NULL marker, so a genuine "\N" value must survive
# without becoming NULL, and a genuine NULL must not become the string.
#
# Written 2026-09-16, ahead of replacing the Vec<Vec<String>> row buffer with
# direct serialisation into a reusable TSV buffer. That change rewrites every
# call site of the escaping, and nothing in the suite covered it: the existing
# tests use well-behaved identifiers and numbers.
#
# Comparison is by md5 on both sides rather than by shipping the values back
# through this script, so the test harness cannot itself mangle what it checks.
#
# Expects: pg2ch_cdc built, PG and CH running.
# Environment: TEST_PG_HOST, TEST_PG_PORT, TEST_PG_USER, TEST_PG_PASSWORD,
#              TEST_PG_DATABASE, TEST_CH_HOST, TEST_CH_PORT, TEST_CH_USER,
#              TEST_CH_PASSWORD

BIN_DIR="${BIN_DIR:-target/release}"
SCHEMA="test_escape"
MIRROR_NAME="test_escape"
CH_DATABASE="test_escape_ch"

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

echo "=== A table of values that collide with TabSeparated syntax ==="
$PSQL <<'SQL'
CREATE SCHEMA test_escape;
CREATE TABLE test_escape.nasty (
    id    INTEGER PRIMARY KEY,
    label TEXT NOT NULL,
    val   TEXT
);
INSERT INTO test_escape.nasty (id, label, val) VALUES
    (1,  'tab',              E'before\tafter'),
    (2,  'newline',          E'line1\nline2'),
    (3,  'crlf',             E'line1\r\nline2'),
    (4,  'backslash',        E'a\\b'),
    (5,  'literal-N',        E'\\N'),
    (6,  'literal-backslash-t', E'\\t'),
    (7,  'literal-backslash-n', E'\\n'),
    (8,  'real-null',        NULL),
    (9,  'empty',            ''),
    (10, 'utf8',             'héllo — 日本語 🚀'),
    (11, 'quotes',           E'single '' double " backtick `'),
    (12, 'all-at-once',      E'x\ty\nz\\w\\N'),
    (13, 'leading-trailing', E'\t leading and trailing \t'),
    (14, 'long',             repeat('abc\def', 500));
SQL
echo "  $(pgq "SELECT count(*) FROM test_escape.nasty") rows staged"

MIRROR_CONFIG=$(mktemp /tmp/test_escape_XXXXXX.yaml)
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
  binary: ${BINARY_MODE:-false}
  ch_timeout_secs: 120

tables:
  - nasty
EOF

# Compare by md5 on each side: the harness never has to carry the raw bytes.
compare_all() {
    local phase="$1" bad=0
    local pg_md5 ch_md5 label
    while IFS='|' read -r id label pg_md5; do
        ch_md5=$(ch_query "SELECT if(val IS NULL, 'NULL', lower(hex(MD5(val)))) \
                           FROM $CH_DATABASE.nasty FINAL WHERE id = $id AND _pg2ch_is_deleted = 0 \
                           FORMAT TabSeparated" | tr -d '[:space:]')
        if [ "$pg_md5" != "$ch_md5" ]; then
            echo "  MISMATCH [$phase] id=$id ($label): pg=$pg_md5 ch=$ch_md5" >&2
            bad=$((bad+1))
        fi
    done < <($PSQL -At -F'|' -c "SELECT id, label, coalesce(md5(val), 'NULL') FROM $SCHEMA.nasty ORDER BY id")
    [ "$bad" -eq 0 ] || fail "$bad value(s) did not survive $phase"
    echo "  all values byte-identical after $phase"
}

echo "=== Initial load ==="
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
ROWS=$(ch_query "SELECT count() FROM $CH_DATABASE.nasty FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$ROWS" = "14" ] || fail "expected 14 rows after load, got $ROWS"
compare_all "the initial load"

echo "=== The literal \\N must not have become NULL, and NULL must not have become a string ==="
LIT=$(ch_query "SELECT if(val IS NULL, 'IS_NULL', 'IS_TEXT') FROM $CH_DATABASE.nasty FINAL WHERE id = 5" | tr -d '[:space:]')
[ "$LIT" = "IS_TEXT" ] || fail "the literal two-character value \\N was stored as NULL"
NUL=$(ch_query "SELECT if(val IS NULL, 'IS_NULL', 'IS_TEXT') FROM $CH_DATABASE.nasty FINAL WHERE id = 8" | tr -d '[:space:]')
[ "$NUL" = "IS_NULL" ] || fail "a real NULL was stored as text"
echo "  both correct"

echo "=== Same values through the CDC path (INSERT) ==="
$PSQL <<'SQL'
INSERT INTO test_escape.nasty (id, label, val)
SELECT id + 100, label || '-cdc', val FROM test_escape.nasty WHERE id <= 14;
SQL
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
ROWS=$(ch_query "SELECT count() FROM $CH_DATABASE.nasty FINAL WHERE _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$ROWS" = "28" ] || fail "expected 28 rows after CDC insert, got $ROWS"
compare_all "a CDC insert"

echo "=== Same values through the CDC path (UPDATE) ==="
# Rewrite every value to a different nasty value, so the update path is
# exercised rather than re-sending what is already there.
$PSQL -c "UPDATE test_escape.nasty SET val = E'updated\t\\\\N\nvalue\\\\' WHERE id <= 14;"
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
compare_all "a CDC update"

echo "=== And a delete still marks, without corrupting neighbours ==="
$PSQL -c "DELETE FROM test_escape.nasty WHERE id = 12;"
"$BIN_DIR/pg2ch_cdc" --config "$MIRROR_CONFIG" --plain >/dev/null
GONE=$(ch_query "SELECT count() FROM $CH_DATABASE.nasty FINAL WHERE id = 12 AND _pg2ch_is_deleted = 0" | tr -d '[:space:]')
[ "$GONE" = "0" ] || fail "deleted row still visible"
compare_all "a CDC delete"

echo "=== Cleanup ==="
rm -f "$MIRROR_CONFIG"
ch_query "DROP DATABASE IF EXISTS $CH_DATABASE"
$PSQL -c "DROP PUBLICATION IF EXISTS pg2ch_$MIRROR_NAME;"
$PSQL -c "SELECT pg_drop_replication_slot('pg2ch_$MIRROR_NAME') FROM pg_replication_slots WHERE slot_name = 'pg2ch_$MIRROR_NAME';" 2>/dev/null || true
$PSQL -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE;"

echo "=== PASS ==="
