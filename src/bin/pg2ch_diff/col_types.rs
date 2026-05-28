//! Type-aware column stringification for cross-database hash comparison.
//!
//! For each column, produces SQL expressions that yield identical text on both
//! PG and CH. Handles nullable columns, floats (with truncation to avoid
//! serialization differences), timestamps, booleans, etc.

use anyhow::{bail, Result};

/// The initial load (PG→CH via postgresql() table function) introduces small
/// ULP differences in float values. Masking the last N mantissa bits absorbs this.
///
/// Float32: 23 mantissa bits. Mask 6 → 17 remaining → ~5.1 decimal digits.
/// Float64: 52 mantissa bits. Mask 14 → 38 remaining → ~11.4 decimal digits.
///   Empirically tested: 14 bits → 0 false positives on ff_v3_ff_basic_cf (104K rows).
const FLOAT4_MASK_BITS: u32 = 6;
const FLOAT8_MASK_BITS: u32 = 14;

/// Rounding fallback decimal places for floats. Used to re-check rows that
/// fail bit-mask comparison (mantissa overflow: e.g. 0xFFF...F → 0x000...0).
/// Both PG and CH use banker's rounding (IEEE 754 rint) when staying in float.
const FLOAT4_ROUND_DECIMALS: u32 = 2;
const FLOAT8_ROUND_DECIMALS: u32 = 2;

pub struct Column {
    pub name: String,
    pub pg_type: String,
    pub nullable: bool,
    pub is_pk: bool,
    /// PG expression — primary comparison (bit-masked hex for floats)
    pub pg_expr: String,
    /// CH expression — primary comparison
    pub ch_expr: String,
    /// PG expression — rounding fallback for floats (used to re-check rows
    /// that fail the bit-mask comparison due to mantissa overflow)
    pub pg_expr_rounded: String,
    /// CH expression — rounding fallback
    pub ch_expr_rounded: String,
}

/// Discover all columns for a table and build matched PG/CH stringify expressions.
/// Returns (all_columns, pk_column_names_in_order).
/// `decimal_tolerance`: if set, Decimal columns are rounded to absorb snapshot noise.
pub fn build_all_columns(
    pg: &pg2ch_cdc::pg::PgClient,
    schema: &str,
    table: &str,
    decimal_tolerance: Option<f64>,
) -> Result<(Vec<Column>, Vec<String>)> {
    // Identity columns: PK if present, else smallest unique non-partial
    // non-expression index with all NOT NULL columns. Same priority as
    // the orchestrator so JOIN keys match CDC ORDER BY.
    let pk_rows = pg.query(&format!(
        "WITH pick AS ( \
           SELECT i.indrelid, i.indkey \
           FROM pg_index i \
           WHERE i.indrelid = '{}.{}'::regclass \
             AND (i.indisprimary OR ( \
                   i.indisunique AND i.indexprs IS NULL AND i.indpred IS NULL \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM pg_attribute a \
                     WHERE a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                       AND NOT a.attnotnull))) \
           ORDER BY i.indisprimary DESC, array_length(i.indkey, 1) ASC \
           LIMIT 1 \
         ) \
         SELECT a.attname FROM pick p \
         JOIN pg_attribute a ON a.attrelid = p.indrelid AND a.attnum = ANY(p.indkey) \
         ORDER BY array_position(p.indkey, a.attnum)",
        schema, table
    ))?;
    let pk_names: Vec<String> = pk_rows.iter().map(|r| r[0].clone()).collect();

    if pk_names.is_empty() {
        bail!(
            "Table {}.{} has no primary key and no usable unique index \
             (a usable unique index is non-partial, non-expression, with all NOT NULL columns)",
            schema, table
        );
    }

    // Get all columns with types and nullability
    let col_rows = pg.query(&format!(
        "SELECT a.attname, t.typname, NOT a.attnotnull as nullable \
         FROM pg_attribute a \
         JOIN pg_type t ON t.oid = a.atttypid \
         WHERE a.attrelid = '{}.{}'::regclass \
           AND a.attnum > 0 \
           AND NOT a.attisdropped \
         ORDER BY a.attnum",
        schema, table
    ))?;

    let mut columns = Vec::new();
    for row in &col_rows {
        let name = &row[0];
        let pg_type = &row[1];
        let nullable = &row[2] == "t";
        let is_pk = pk_names.contains(name);

        let (pg_expr, ch_expr, pg_expr_rounded, ch_expr_rounded) =
            build_expr(name, pg_type, nullable, decimal_tolerance)?;

        columns.push(Column {
            name: name.clone(),
            pg_type: pg_type.clone(),
            nullable,
            is_pk,
            pg_expr,
            ch_expr,
            pg_expr_rounded,
            ch_expr_rounded,
        });
    }

    Ok((columns, pk_names))
}

/// Returns (pg_expr, ch_expr, pg_expr_rounded, ch_expr_rounded).
/// For non-float types, the rounded expressions are identical to the primary ones.
fn build_expr(name: &str, pg_type: &str, nullable: bool, decimal_tolerance: Option<f64>) -> Result<(String, String, String, String)> {
    let (pg_raw, ch_raw, pg_round, ch_round) = match pg_type {
        // Integers: both sides produce decimal string
        "int2" | "int4" | "int8" => {
            let pg = format!("{}::text", name);
            let ch = format!("toString({})", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        // Float4/8: primary uses bit-masked hex for speed.
        // Rounded fallback uses banker's rounding via round(col*10^N)/10^N
        // (both PG and CH use IEEE 754 rint) to re-check mantissa overflow cases.
        "float4" => {
            let mask = (1u64 << FLOAT4_MASK_BITS) - 1;
            let scale = format!("1e{}", FLOAT4_ROUND_DECIMALS);
            (
                format!(
                    "lpad(to_hex(('x' || encode(float4send({n}), 'hex'))::bit(32)::bigint & (~{mask}::bigint)), 8, '0')",
                    n = name, mask = mask
                ),
                format!(
                    "leftPad(lower(hex(bitAnd(reinterpretAsUInt32({n}), bitNot(toUInt32({mask}))))), 8, '0')",
                    n = name, mask = mask
                ),
                format!("(round({n} * {s}) / {s})::text", n = name, s = scale),
                format!("toString(round({n}, {d}))", n = name, d = FLOAT4_ROUND_DECIMALS),
            )
        }

        "float8" => {
            let mask = (1u64 << FLOAT8_MASK_BITS) - 1;
            let scale = format!("1e{}", FLOAT8_ROUND_DECIMALS);
            (
                format!(
                    "lpad(to_hex(('x' || encode(float8send({n}), 'hex'))::bit(64)::bigint & (~{mask}::bigint)), 16, '0')",
                    n = name, mask = mask
                ),
                format!(
                    "leftPad(lower(hex(bitAnd(reinterpretAsUInt64({n}), bitNot(toUInt64({mask}))))), 16, '0')",
                    n = name, mask = mask
                ),
                format!("(round({n} * {s}) / {s})::text", n = name, s = scale),
                format!("toString(round({n}, {d}))", n = name, d = FLOAT8_ROUND_DECIMALS),
            )
        }

        // Numeric/Decimal: both sides trim trailing zeros similarly.
        // The postgresql() snapshot can introduce small rounding errors vs CDC
        // at the Decimal scale boundary. The rounded fallback truncates to
        // fewer decimal places to absorb this.
        "numeric" => {
            let pg = format!("{}::text", name);
            let ch = format!("toString({})", name);
            if let Some(tol) = decimal_tolerance {
                // Round to one fewer decimal place than the tolerance implies,
                // so that values within ±tolerance always round to the same result.
                // 0.0001 → 3 decimals, 0.01 → 1 decimal, 0.1 → 0 decimals
                let decimals = ((-tol.log10()).ceil() - 1.0).max(0.0) as u32;
                let pg_r = format!("round({}::numeric, {})::text", name, decimals);
                let ch_r = format!("toString(round({}, {}))", name, decimals);
                (pg, ch, pg_r, ch_r)
            } else {
                (pg.clone(), ch.clone(), pg, ch)
            }
        }

        // Text types: already text
        "varchar" | "text" | "bpchar" => {
            let pg = format!("{}::text", name);
            let ch = format!("toString({})", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        // Date: YYYY-MM-DD on both sides
        // PG: clamp to CH DateTime64 max to match overflow behavior
        // Note: LEAST(NULL, x) returns x in PG (skips NULLs), so clamp must be inside CASE
        // CH: substring(toString()) is safe for all dates (formatDateTime breaks pre-1970)
        "date" => {
            let pg = format!(
                "CASE WHEN {} IS NOT NULL THEN to_char(LEAST({}, '2299-12-31'::date), 'YYYY-MM-DD') END",
                name, name
            );
            let ch = format!("substring(toString({}), 1, 10)", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        // Timestamp without tz: YYYY-MM-DD HH:MI:SS (no fractional seconds)
        "timestamp" => {
            let pg = format!(
                "CASE WHEN {} IS NOT NULL THEN to_char(LEAST({}, '2299-12-31'::timestamp), 'YYYY-MM-DD HH24:MI:SS') END",
                name, name
            );
            let ch = format!("substring(toString({}), 1, 19)", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        // Timestamp with tz: convert to UTC, strip offset
        "timestamptz" => {
            let pg = format!(
                "CASE WHEN {} IS NOT NULL THEN to_char(LEAST({} AT TIME ZONE 'UTC', '2299-12-31'::timestamp), 'YYYY-MM-DD HH24:MI:SS') END",
                name, name
            );
            let ch = format!("substring(toString({}), 1, 19)", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        // Boolean: normalize to 1/0
        "bool" => {
            let pg = format!("CASE WHEN {} THEN '1' ELSE '0' END", name);
            let ch = format!("toString(toUInt8({}))", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        // UUID: lowercase hex with dashes
        "uuid" => {
            let pg = format!("{}::text", name);
            let ch = format!("toString({})", name);
            (pg.clone(), ch.clone(), pg, ch)
        }

        other => bail!(
            "Column '{}' has unsupported type '{}' — add support in col_types.rs",
            name, other
        ),
    };

    if nullable {
        Ok((
            format!("COALESCE({}, '<NULL>')", pg_raw),
            format!("ifNull({}, '<NULL>')", ch_raw),
            format!("COALESCE({}, '<NULL>')", pg_round),
            format!("ifNull({}, '<NULL>')", ch_round),
        ))
    } else {
        Ok((pg_raw, ch_raw, pg_round, ch_round))
    }
}

/// Build the PG concat expression: concat_ws('|', expr1, expr2, ...)
pub fn pg_concat_expr(columns: &[&Column]) -> String {
    let parts: Vec<&str> = columns.iter().map(|c| c.pg_expr.as_str()).collect();
    format!("concat_ws('|', {})", parts.join(", "))
}

/// Build the CH concat expression: arrayStringConcat([expr1, expr2, ...], '|')
pub fn ch_concat_expr(columns: &[&Column]) -> String {
    let parts: Vec<&str> = columns.iter().map(|c| c.ch_expr.as_str()).collect();
    format!("arrayStringConcat([{}], '|')", parts.join(", "))
}

/// PG concat using rounded fallback expressions (for re-checking bit-mask failures)
pub fn pg_concat_expr_rounded(columns: &[&Column]) -> String {
    let parts: Vec<&str> = columns.iter().map(|c| c.pg_expr_rounded.as_str()).collect();
    format!("concat_ws('|', {})", parts.join(", "))
}

/// CH concat using rounded fallback expressions
pub fn ch_concat_expr_rounded(columns: &[&Column]) -> String {
    let parts: Vec<&str> = columns.iter().map(|c| c.ch_expr_rounded.as_str()).collect();
    format!("arrayStringConcat([{}], '|')", parts.join(", "))
}

/// PG hash aggregate: sum of last-8-hex-of-md5 as bigint
pub fn pg_hash_agg(concat_expr: &str) -> String {
    format!(
        "sum(('x' || substr(md5({}), 25, 8))::bit(32)::bigint)",
        concat_expr
    )
}

/// CH hash aggregate: sum of last-8-hex-of-MD5 as big-endian UInt32 (matches PG's bit(32)::bigint)
pub fn ch_hash_agg(concat_expr: &str) -> String {
    format!(
        "sum(reinterpretAsUInt32(reverse(unhex(substr(hex(MD5({})), 25, 8)))))",
        concat_expr
    )
}
