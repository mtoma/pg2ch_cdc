//! Type-aware PK stringification for cross-database hash comparison.
//!
//! For each PK column, produces SQL cast expressions that yield identical
//! text on both PG and CH. Rejects types where exact text matching is unsafe.

use anyhow::{bail, Result};
use tracing::warn;

/// Supported PK column types and their canonical stringify expressions.
/// The contract: pg_expr(value) == ch_expr(value) for all valid values.
pub struct PkColumn {
    pub name: String,
    pub pg_type: String,
    pub pg_expr: String,
    pub ch_expr: String,
}

/// Query PG for PK column names and types, build matched stringify expressions.
pub fn build_pk_columns(
    pg: &pg2ch_cdc::pg::PgClient,
    schema: &str,
    table: &str,
) -> Result<Vec<PkColumn>> {
    let rows = pg.query(&format!(
        "SELECT a.attname, t.typname \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         JOIN pg_type t ON t.oid = a.atttypid \
         WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey, a.attnum)",
        schema, table
    ))?;

    if rows.is_empty() {
        bail!("Table {}.{} has no primary key", schema, table);
    }

    let mut columns = Vec::new();
    for row in &rows {
        let name = &row[0];
        let pg_type = &row[1];
        let col = build_column(name, pg_type)?;
        columns.push(col);
    }

    Ok(columns)
}

fn build_column(name: &str, pg_type: &str) -> Result<PkColumn> {
    let (pg_expr, ch_expr) = match pg_type {
        // Integers: both sides produce decimal string
        "int2" | "int4" | "int8" => (
            format!("{}::text", name),
            format!("toString({})", name),
        ),

        // Text/varchar: already text, no conversion needed
        "varchar" | "text" | "bpchar" => (
            format!("{}::text", name),
            format!("toString({})", name),
        ),

        // Date: both sides produce YYYY-MM-DD
        // CH: substring(toString()) is safe for all dates; formatDateTime is broken pre-1970
        // PG: clamp to DateTime64(6) max (2299-12-31) to match CH overflow behavior
        "date" => (
            format!("to_char(LEAST({}, '2299-12-31'::date), 'YYYY-MM-DD')", name),
            format!("substring(toString({}), 1, 10)", name),
        ),

        // Timestamp (without tz): force YYYY-MM-DD HH:MI:SS (no fractional seconds)
        // CH: toString() gives 'YYYY-MM-DD HH:MM:SS.000000', take first 19 chars
        // PG: clamp to DateTime64(6) max (2299-12-31 00:00:00) to match CH overflow behavior
        "timestamp" => (
            format!("to_char(LEAST({}, '2299-12-31'::timestamp), 'YYYY-MM-DD HH24:MI:SS')", name),
            format!("substring(toString({}), 1, 19)", name),
        ),

        // Timestamp with timezone: convert to UTC, format without tz
        // PG: clamp to DateTime64(6) max to match CH overflow behavior
        "timestamptz" => (
            format!("to_char(LEAST({} AT TIME ZONE 'UTC', '2299-12-31'::timestamp), 'YYYY-MM-DD HH24:MI:SS')", name),
            format!("substring(toString({}), 1, 19)", name),
        ),

        // Numeric: warn — works for integer-valued numerics (common in PKs)
        // but would silently differ for fractional values.
        "numeric" => {
            warn!(
                "PK column '{}' is numeric — hash comparison assumes integer values (no fractional part)",
                name
            );
            // PG: trims trailing zeros. Force integer output.
            // CH: Decimal toString also trims. Should match for integer values.
            (
                format!("trunc({})::bigint::text", name),
                format!("toString(toInt64({}))", name),
            )
        }

        // UUID: both produce lowercase hex with dashes
        "uuid" => (
            format!("{}::text", name),
            format!("toString({})", name),
        ),

        // Boolean: normalize to 1/0
        "bool" => (
            format!("CASE WHEN {} THEN '1' ELSE '0' END", name),
            format!("toString(toUInt8({}))", name),
        ),

        // Unsupported types
        "float4" | "float8" => {
            bail!(
                "PK column '{}' has type '{}' — floating point types are not supported \
                 for hash comparison (text representation may differ between PG and CH)",
                name, pg_type
            );
        }

        other => {
            bail!(
                "PK column '{}' has unsupported type '{}' — add support in pk_types.rs",
                name, other
            );
        }
    };

    Ok(PkColumn {
        name: name.to_string(),
        pg_type: pg_type.to_string(),
        pg_expr,
        ch_expr,
    })
}

/// Build the PG concat expression: concat_ws('|', expr1, expr2, ...)
pub fn pg_concat_expr(columns: &[PkColumn]) -> String {
    let parts: Vec<&str> = columns.iter().map(|c| c.pg_expr.as_str()).collect();
    format!("concat_ws('|', {})", parts.join(", "))
}

/// Build the CH concat expression: concat(expr1, '|', expr2, '|', ...)
pub fn ch_concat_expr(columns: &[PkColumn]) -> String {
    if columns.len() == 1 {
        return columns[0].ch_expr.clone();
    }
    let mut expr = columns[0].ch_expr.clone();
    for col in &columns[1..] {
        expr = format!("concat({}, '|', {})", expr, col.ch_expr);
    }
    expr
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
