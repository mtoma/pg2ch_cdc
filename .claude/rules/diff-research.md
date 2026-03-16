# pg2ch_diff — Research & Design Reference

## Problem

Validate that ClickHouse mirrors faithfully reflect PostgreSQL source tables after CDC replication. The diff tool must work cross-database (PG ↔ CH) with different SQL dialects, type systems, and hash functions.

## Constraint: PostgreSQL is the bottleneck

ClickHouse can scan billions of rows in seconds (columnar, optimized for analytics). PostgreSQL is OLTP — full table scans compete with production workload, hold back vacuum (MVCC), and are orders of magnitude slower. **Every algorithm choice must minimize PG load.**

## Progressive validation levels

The tool uses escalating levels of complexity and load. Each level gives more information but costs more. Users configure per-table which level to run.

### Level 1: Metadata count (near-zero load)

```sql
-- PG: estimate from planner statistics (no table scan)
SELECT reltuples::bigint FROM pg_class
WHERE relname = 'table_name' AND relnamespace = 'schema'::regnamespace;

-- CH: estimate from metadata (no FINAL, no dedup)
SELECT count() FROM table SETTINGS final = 0;
```

**Cost:** Instant on both sides. No table scan.
**Accuracy:** Approximate. PG reltuples can be stale (updated by ANALYZE/autovacuum). CH count without FINAL includes not-yet-merged duplicates and soft-deleted rows.
**Detects:** Gross mismatches (empty table, order-of-magnitude difference).

### Level 2: Exact count

```sql
-- PG: real count (full sequential scan or index-only scan on PK)
SELECT count(*) FROM table;

-- CH: exact count with deduplication
SELECT count() FROM table FINAL WHERE _pg2ch_is_deleted = 0;
```

**Cost:** PG full scan (expensive on large tables). CH FINAL forces merge (moderate cost).
**Accuracy:** Exact row counts.
**Detects:** Missing or extra rows. Does not detect mutations (row exists on both sides but with different values).

### Level 3: Primary key comparison

Compare the set of primary keys between PG and CH. Finds rows that exist on one side but not the other.

**Approach:** Stream sorted PKs from both sides, merge-compare:
- PK in PG but not CH → missing in CH
- PK in CH but not PG → extra in CH (or deleted in PG but not yet replicated)

```sql
-- PG: stream PKs sorted
SELECT pk_col1, pk_col2 FROM table ORDER BY pk_col1, pk_col2;

-- CH: stream PKs sorted, excluding soft-deleted
SELECT pk_col1, pk_col2 FROM table FINAL
WHERE _pg2ch_is_deleted = 0 ORDER BY pk_col1, pk_col2;
```

**Cost:** PG index-only scan on PK (if HOT-updated and vacuumed). CH very fast.
**Accuracy:** Exact set difference on PKs.
**Detects:** Missing rows, extra rows, phantom deletes. Does not detect value mutations on non-PK columns.
**Best for:** Append-only tables where rows are never updated, only inserted.

### Level 4: Chunked checksum (future)

Divide PK range into N chunks, checksum each chunk on both sides using MD5 (available in both PG and CH). Compare checksums to find differing chunks.

**Open questions for future implementation:**
- Column selection: checksum all columns or a subset?
- Type normalization: floating point values, timestamps with varying precision, NULLs
- MD5 vs other hash functions (MD5 is the only one common to both PG and CH)
- Canonical text representation for each type to ensure PG and CH produce identical hashes

### Level 5: Row-level comparison (future)

For chunks that differ, fetch full rows and compare locally. Last resort — highest accuracy, highest cost.

**Open questions:**
- Floating point comparison: epsilon-based equality
- Timestamp precision: PG microseconds vs CH varying precision (DateTime64(6) etc.)
- NULL handling: PG NULL vs CH Nullable() default values
- Text encoding: collation differences

## Existing tools

### data-diff (Datafold) — archived

Python-based, used bisection algorithm: divide PK range into segments, checksum each segment with MD5, recursively bisect mismatching segments. Benchmarked at 1B rows in ~5 minutes when tables match. Supports PG and CH. **Archived May 2024, no longer maintained.**

### reladiff (erezsh)

Maintained fork of data-diff by its original author. Same bisection algorithm. Active development. Python with its own DB connectors.

### pgCompare (Crunchy Data)

Java-based, PG-centric. No ClickHouse support. Record-level comparison, modest performance.

## Cross-database hash compatibility

The fundamental challenge: PG and CH have different type representations and hash functions.

**MD5** is the only hash available in both:
- PG: `md5(text)` → 32-char hex string
- CH: `MD5(string)` → 16-byte binary, or `hex(MD5(string))` → 32-char hex

To get matching hashes, every column must be cast to an identical text representation on both sides. This requires careful handling of:
- **Floats:** `0.1 + 0.2` may render as `0.30000000000000004` on one side and `0.3` on the other
- **Timestamps:** PG `2024-01-01 00:00:00+00` vs CH `2024-01-01 00:00:00.000000`
- **NULLs:** PG `NULL` vs CH empty string in Nullable columns
- **Booleans:** PG `t/f` vs CH `1/0`
- **Decimals:** trailing zeros, scientific notation

This is why hash-based approaches (levels 4-5) are deferred — they require a complete, tested type normalization layer.

## Performance reference points

| Approach | PG load (1B rows) | CH load (1B rows) | Detects |
|---|---|---|---|
| Metadata count | Instant | Instant | Gross mismatches |
| Exact count | Minutes (seq scan) | Seconds (FINAL) | Missing/extra rows |
| PK comparison | Minutes (index scan) | Seconds | Missing/extra PKs |
| Chunked checksum | Minutes (partial scans) | Seconds | Which chunks differ |
| Row-level fetch | Hours | Minutes | Exact differences |

## References

- [data-diff technical explanation](https://github.com/datafold/data-diff/blob/master/docs/technical-explanation.md)
- [reladiff](https://github.com/erezsh/reladiff)
- [Using checksums to verify 100M records (Sirupsen)](https://sirupsen.com/napkin/problem-14-using-checksums-to-verify)
- [Hashing tables for consistency (Sisense)](https://www.sisense.com/blog/hashing-tables-to-ensure-consistency-in-postgres-redshift-and-mysql/)
- [ClickHouse hash functions](https://clickhouse.com/docs/sql-reference/functions/hash-functions)
