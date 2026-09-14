//! PostgreSQL → ClickHouse column type mapping.
//!
//! # Why this exists rather than deferring to `postgresql()`
//!
//! Until 2026-09-14 the tool took its column types from
//! `DESCRIBE TABLE postgresql(...)`, on the reasoning that ClickHouse's own
//! reader should decide. That is comfortable but it surrenders two things we
//! turn out to need.
//!
//! It decides badly for an unconstrained `numeric`. PostgreSQL's `numeric`
//! with no declared precision is arbitrary-precision; ClickHouse maps it to
//! `Decimal(38, 19)`, leaving 19 digits before the point. `cstat.sec_dtrt.trfd`
//! held `17485804441876462000` — twenty digits, in a column whose other values
//! are around 1.0 — and the load died with `Code: 69, Decimal value is too
//! big`. Because that aborts the batch it took the whole run with it: one row
//! in 2,382,042 blocked a 149-table mirror for 3h31m and left the slot 74 GB
//! behind.
//!
//! And it gives us nowhere to stand when a source column needs a type the
//! reader would not pick. With the structure declared by us, the load reads
//! through `CREATE TABLE ... ENGINE = PostgreSQL` with our types, which also
//! measured ~3x faster than the table function (1481 ms vs 4760 ms server-side
//! over 2.38M rows) because the reader does less parsing.
//!
//! # Scope
//!
//! Deliberately covers exactly the types present in the mirrored schemas and
//! refuses anything else by name. A silent fallback is how you get a column
//! quietly stored as the wrong thing; an unmapped type should stop the run and
//! be added here on purpose.
//!
//! Measured across `ciq`, `cstat` and `fds` on 2026-09-14:
//!
//! ```text
//! character varying  6185 cols    timestamp without tz  1016
//! smallint           5057         double precision       646
//! numeric            4587 (3 unconstrained)   character  171
//! integer            2842         bigint                 100
//! date                 71         text                     8      real  5
//! ```
//!
//! # Relationship to the rest of the data lake
//!
//! `DLTools.py` holds the lake's long-standing mapping, used by the 2IQ,
//! Bloomberg and FactSet exporters. This is a deliberate improvement on it,
//! not a copy. Two differences matter:
//!
//! - `double precision` there maps to `DECIMAL`. That is wrong: a binary float
//!   is not a decimal, and the conversion changes both the value and the
//!   semantics on 646 columns. Here it is `Float64`.
//! - `timestamp` there maps to `DateTime` — whole seconds, no timezone. That
//!   would discard microseconds and reopen the DST defect repaired across the
//!   13 tables migrated earlier this month. Here it stays
//!   `DateTime64(6, <tz>)`.
//!
//! Its unconstrained-`numeric` fallback of `DECIMAL(50, 10)` would have held
//! `trfd` (40 integer digits), but truncates to 10 decimal places, and `trfd`'s
//! measured scale is 16 — trading a loud crash for silent precision loss. Scale
//! 19 is kept here.

use anyhow::{bail, Result};

/// One column as PostgreSQL's catalog describes it.
#[derive(Debug, Clone)]
pub struct PgColumn {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    /// `numeric_precision` — `None` for an unconstrained `numeric`.
    pub numeric_precision: Option<u32>,
    /// `numeric_scale` — `None` for an unconstrained `numeric`.
    pub numeric_scale: Option<u32>,
}

/// Precision used for a PostgreSQL `numeric` that declares none.
///
/// 76 is ClickHouse's maximum (Decimal256). With scale 19 that leaves 57
/// digits before the point, against the 20 that broke the mirror. It does not
/// make overflow impossible, only implausible — beyond 57 integer digits the
/// load still fails, which is right: silently truncating a number the source
/// really holds would be worse than stopping.
const UNCONSTRAINED_NUMERIC_PRECISION: u32 = 76;

/// Scale used for a PostgreSQL `numeric` that declares none. Matches what
/// ClickHouse itself chooses, so migrating a column between the two mappings
/// does not move the decimal point.
const UNCONSTRAINED_NUMERIC_SCALE: u32 = 19;

/// The ClickHouse type for one PostgreSQL column.
///
/// `tz` is written into any `DateTime64` produced for a naive `timestamp`; it
/// is the mirror's `store_naive_timestamps_as_timezone`. `timestamptz` ignores
/// it and pins UTC, because a `timestamptz` is an instant and UTC is the only
/// reading of it that is not a display choice.
///
/// `force_not_null` is for primary-key columns: ClickHouse cannot order by a
/// Nullable column, and the tool already refuses tables whose PK is nullable
/// in PostgreSQL.
pub fn ch_type_for(col: &PgColumn, tz: &str, force_not_null: bool) -> Result<String> {
    let base = match col.data_type.as_str() {
        "character varying" | "character" | "text" => "String".to_string(),

        "smallint" => "Int16".to_string(),
        "integer" => "Int32".to_string(),
        "bigint" => "Int64".to_string(),

        "real" => "Float32".to_string(),
        // NOT Decimal. A double is binary floating point; rendering it as a
        // decimal invents precision it never had and changes comparisons.
        "double precision" => "Float64".to_string(),

        "numeric" => {
            let p = col.numeric_precision.unwrap_or(UNCONSTRAINED_NUMERIC_PRECISION);
            let s = col.numeric_scale.unwrap_or(UNCONSTRAINED_NUMERIC_SCALE);
            if p == 0 || p > 76 {
                bail!(
                    "column {}: numeric precision {} is outside ClickHouse's Decimal range (1-76)",
                    col.name, p
                );
            }
            if s > p {
                bail!(
                    "column {}: numeric scale {} exceeds precision {}",
                    col.name, s, p
                );
            }
            format!("Decimal({}, {})", p, s)
        }

        // Date32 spans 1900-2299 against Date's 1970-2149. PostgreSQL's range
        // is far wider still and these schemas carry sentinel dates, so a value
        // outside Date32 will fail the load rather than wrap silently.
        "date" => "Date32".to_string(),

        // Microseconds, and a timezone that travels with the column. See
        // .claude/rules/timezones.md for why an unstated timezone is a trap.
        "timestamp without time zone" => format!("DateTime64(6, '{}')", tz),
        "timestamp with time zone" => "DateTime64(6, 'UTC')".to_string(),

        other => bail!(
            "column {}: unmapped PostgreSQL type '{}'. Add it to typemap.rs \
             deliberately rather than letting it fall through to something plausible.",
            col.name, other
        ),
    };

    Ok(if col.is_nullable && !force_not_null {
        format!("Nullable({})", base)
    } else {
        base
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(data_type: &str, nullable: bool, p: Option<u32>, s: Option<u32>) -> PgColumn {
        PgColumn {
            name: "c".to_string(),
            data_type: data_type.to_string(),
            is_nullable: nullable,
            numeric_precision: p,
            numeric_scale: s,
        }
    }

    #[test]
    fn maps_every_type_present_in_the_mirrored_schemas() {
        let cases = [
            ("character varying", "String"),
            ("character", "String"),
            ("text", "String"),
            ("smallint", "Int16"),
            ("integer", "Int32"),
            ("bigint", "Int64"),
            ("real", "Float32"),
            ("double precision", "Float64"),
            ("date", "Date32"),
        ];
        for (pg, ch) in cases {
            assert_eq!(ch_type_for(&col(pg, false, None, None), "UTC", false).unwrap(), ch);
        }
    }

    #[test]
    fn a_double_is_a_float_not_a_decimal() {
        // DLTools.py maps `double precision` to DECIMAL on 646 columns. A
        // binary float is not a decimal and must not be stored as one.
        let t = ch_type_for(&col("double precision", true, Some(53), None), "UTC", false).unwrap();
        assert_eq!(t, "Nullable(Float64)");
        assert!(!t.contains("Decimal"));
    }

    #[test]
    fn declared_numeric_precision_is_preserved_exactly() {
        assert_eq!(
            ch_type_for(&col("numeric", false, Some(38), Some(30)), "UTC", false).unwrap(),
            "Decimal(38, 30)"
        );
        assert_eq!(
            ch_type_for(&col("numeric", true, Some(10), Some(2)), "UTC", false).unwrap(),
            "Nullable(Decimal(10, 2))"
        );
    }

    #[test]
    fn unconstrained_numeric_gets_room_for_the_row_that_broke_the_mirror() {
        // cstat.sec_dtrt.trfd = 17485804441876462000 — 20 integer digits.
        let t = ch_type_for(&col("numeric", true, None, None), "UTC", false).unwrap();
        assert_eq!(t, "Nullable(Decimal(76, 19))");
        let int_digits = 76 - 19;
        assert!(int_digits >= 20, "must hold 20 integer digits, has {}", int_digits);
    }

    #[test]
    fn timestamps_keep_microseconds_and_carry_their_timezone() {
        assert_eq!(
            ch_type_for(&col("timestamp without time zone", true, None, None), "Europe/Paris", false).unwrap(),
            "Nullable(DateTime64(6, 'Europe/Paris'))"
        );
        // A timestamptz is an instant; UTC is not a display preference.
        assert_eq!(
            ch_type_for(&col("timestamp with time zone", false, None, None), "Europe/Paris", false).unwrap(),
            "DateTime64(6, 'UTC')"
        );
    }

    #[test]
    fn primary_key_columns_are_never_nullable() {
        assert_eq!(ch_type_for(&col("integer", true, None, None), "UTC", true).unwrap(), "Int32");
        assert_eq!(ch_type_for(&col("integer", true, None, None), "UTC", false).unwrap(), "Nullable(Int32)");
    }

    #[test]
    fn an_unmapped_type_stops_the_run_instead_of_guessing() {
        let e = ch_type_for(&col("jsonb", false, None, None), "UTC", false).unwrap_err().to_string();
        assert!(e.contains("jsonb"), "error must name the type: {}", e);
        assert!(e.contains("unmapped"), "error must say it is unmapped: {}", e);
    }

    #[test]
    fn impossible_numeric_parameters_are_refused() {
        assert!(ch_type_for(&col("numeric", false, Some(99), Some(2)), "UTC", false).is_err());
        assert!(ch_type_for(&col("numeric", false, Some(10), Some(20)), "UTC", false).is_err());
    }
}
