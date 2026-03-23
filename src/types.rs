//! Type conversion utilities for PostgreSQL → ClickHouse CDC.
//!
//! Handles: text-mode value mapping (bool, timestamptz), binary wire format
//! decoding (all supported OIDs), numeric base-10000 decoding, delete row
//! default values, and tuple-to-string conversion.

use std::collections::HashMap;
use tracing::warn;

use crate::pgoutput::{RelationInfo, TupleData};

// ── Default values for delete rows ──────────────────────────────────────

pub fn default_for_oid(oid: u32) -> &'static str {
    match oid {
        16 => "false",
        20 | 21 | 23 => "0",
        700 | 701 | 1700 => "0",
        25 | 1043 => "",
        1082 => "1970-01-01",
        1114 => "1970-01-01 00:00:00",
        1184 => "1970-01-01 00:00:00",
        2950 => "00000000-0000-0000-0000-000000000000",
        _ => "",
    }
}

pub fn build_delete_row(key_values: &[TupleData], rel: &RelationInfo) -> Vec<String> {
    let key_cols: Vec<usize> = rel
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.flags & 1 != 0)
        .map(|(i, _)| i)
        .collect();

    let mut key_map = HashMap::new();
    for &col_idx in &key_cols {
        if col_idx < key_values.len() {
            let oid = rel.columns.get(col_idx).map(|c| c.type_oid).unwrap_or(0);
            let val = match &key_values[col_idx] {
                TupleData::Text(s) => {
                    if oid == 1184 { timestamptz_to_utc(s) } else { s.clone() }
                }
                TupleData::Binary(data) => decode_binary_value(oid, data),
                TupleData::Null | TupleData::Unchanged => "\\N".to_string(),
            };
            key_map.insert(col_idx, val);
        }
    }

    rel.columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            key_map
                .get(&i)
                .cloned()
                .unwrap_or_else(|| default_for_oid(col.type_oid).to_string())
        })
        .collect()
}

// ── Timestamptz conversion ──────────────────────────────────────────────

/// Convert a PostgreSQL timestamptz string to UTC and strip the offset.
/// PostgreSQL sends "2026-03-11 15:32:14.085517+01" — we parse it,
/// convert to UTC, and return "2026-03-11 14:32:14.085517".
pub fn timestamptz_to_utc(s: &str) -> String {
    let normalized = normalize_pg_offset(s);
    match chrono::DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f%:z") {
        Ok(dt) => {
            let utc = dt.with_timezone(&chrono::Utc);
            utc.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
        }
        Err(_) => {
            warn!("Failed to parse timestamptz '{}', passing through as-is", s);
            s.to_string()
        }
    }
}

/// Normalize PostgreSQL timezone offsets to chrono-compatible format.
/// "2026-03-11 10:30:00+01" → "2026-03-11 10:30:00+01:00"
/// "2026-03-11 10:30:00+05:30" stays as-is
fn normalize_pg_offset(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut tz_pos = None;
    for i in (1..bytes.len()).rev() {
        if (bytes[i] == b'+' || bytes[i] == b'-') && bytes[i - 1].is_ascii_digit() {
            tz_pos = Some(i);
            break;
        }
    }
    let Some(pos) = tz_pos else {
        return s.to_string();
    };
    let offset_part = &s[pos..];
    if offset_part.len() == 3
        && offset_part.as_bytes()[1].is_ascii_digit()
        && offset_part.as_bytes()[2].is_ascii_digit()
    {
        return format!("{}:00", s);
    }
    s.to_string()
}

// ── PostgreSQL binary wire format decoder ───────────────────────────────

/// PG epoch is 2000-01-01 00:00:00 UTC (946684800 Unix seconds).
const PG_EPOCH_UNIX: i64 = 946_684_800;

/// Decode a PostgreSQL binary wire value to its string representation.
pub fn decode_binary_value(oid: u32, data: &[u8]) -> String {
    match oid {
        // bool — 1 byte → ClickHouse UInt8 (1/0)
        16 => {
            if data.len() == 1 {
                if data[0] != 0 { "1".to_string() } else { "0".to_string() }
            } else {
                warn!("Invalid bool binary length: {}", data.len());
                "0".to_string()
            }
        }
        // int2 — 2 bytes big-endian
        21 => {
            if data.len() == 2 {
                i16::from_be_bytes(data.try_into().unwrap()).to_string()
            } else {
                "0".to_string()
            }
        }
        // int4 — 4 bytes big-endian
        23 => {
            if data.len() == 4 {
                i32::from_be_bytes(data.try_into().unwrap()).to_string()
            } else {
                "0".to_string()
            }
        }
        // int8 — 8 bytes big-endian
        20 => {
            if data.len() == 8 {
                i64::from_be_bytes(data.try_into().unwrap()).to_string()
            } else {
                "0".to_string()
            }
        }
        // float4 — 4 bytes IEEE 754
        700 => {
            if data.len() == 4 {
                f32::from_be_bytes(data.try_into().unwrap()).to_string()
            } else {
                "0".to_string()
            }
        }
        // float8 — 8 bytes IEEE 754
        701 => {
            if data.len() == 8 {
                f64::from_be_bytes(data.try_into().unwrap()).to_string()
            } else {
                "0".to_string()
            }
        }
        // text (25), varchar (1043) — raw UTF-8 bytes
        25 | 1043 => String::from_utf8_lossy(data).into_owned(),
        // date — 4 bytes i32, days since 2000-01-01
        1082 => {
            if data.len() == 4 {
                let days = i32::from_be_bytes(data.try_into().unwrap());
                let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                match epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
                    Some(d) => d.format("%Y-%m-%d").to_string(),
                    None => "1970-01-01".to_string(),
                }
            } else {
                "1970-01-01".to_string()
            }
        }
        // timestamp (1114) — 8 bytes i64, microseconds since 2000-01-01 00:00:00
        1114 => {
            if data.len() == 8 {
                let us = i64::from_be_bytes(data.try_into().unwrap());
                let secs = us / 1_000_000 + PG_EPOCH_UNIX;
                let nsecs = ((us % 1_000_000) * 1000) as u32;
                match chrono::DateTime::from_timestamp(secs, nsecs) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                    None => "1970-01-01 00:00:00.000000".to_string(),
                }
            } else {
                "1970-01-01 00:00:00.000000".to_string()
            }
        }
        // timestamptz (1184) — same encoding as timestamp, already in UTC
        1184 => {
            if data.len() == 8 {
                let us = i64::from_be_bytes(data.try_into().unwrap());
                let secs = us / 1_000_000 + PG_EPOCH_UNIX;
                let nsecs = ((us % 1_000_000) * 1000) as u32;
                match chrono::DateTime::from_timestamp(secs, nsecs) {
                    Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                    None => "1970-01-01 00:00:00.000000".to_string(),
                }
            } else {
                "1970-01-01 00:00:00.000000".to_string()
            }
        }
        // uuid — 16 bytes raw
        2950 => {
            if data.len() == 16 {
                format!(
                    "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                    u32::from_be_bytes(data[0..4].try_into().unwrap()),
                    u16::from_be_bytes(data[4..6].try_into().unwrap()),
                    u16::from_be_bytes(data[6..8].try_into().unwrap()),
                    u16::from_be_bytes(data[8..10].try_into().unwrap()),
                    (data[10] as u64) << 40
                        | (data[11] as u64) << 32
                        | (data[12] as u64) << 24
                        | (data[13] as u64) << 16
                        | (data[14] as u64) << 8
                        | data[15] as u64,
                )
            } else {
                "00000000-0000-0000-0000-000000000000".to_string()
            }
        }
        // numeric (1700) — variable-length base-10000 format
        1700 => decode_pg_numeric(data),
        // Unknown OID — pass through as hex-escaped string
        _ => {
            warn!("Unknown binary OID {}, passing as UTF-8 lossy", oid);
            String::from_utf8_lossy(data).into_owned()
        }
    }
}

/// Decode PostgreSQL binary NUMERIC format to a decimal string.
///
/// Format: [ndigits: u16] [weight: i16] [sign: u16] [dscale: u16] [digits: u16 × ndigits]
///
/// Each digit is a base-10000 value (0-9999).
/// Weight is the exponent of the first digit: value = sum(digit[i] * 10000^(weight - i))
/// Sign: 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
/// dscale = number of digits after the decimal point in the original representation
fn decode_pg_numeric(data: &[u8]) -> String {
    if data.len() < 8 {
        warn!("Numeric binary data too short: {} bytes", data.len());
        return "0".to_string();
    }

    let ndigits = u16::from_be_bytes(data[0..2].try_into().unwrap()) as usize;
    let weight = i16::from_be_bytes(data[2..4].try_into().unwrap());
    let sign = u16::from_be_bytes(data[4..6].try_into().unwrap());
    let dscale = u16::from_be_bytes(data[6..8].try_into().unwrap()) as usize;

    // NaN
    if sign == 0xC000 {
        return "NaN".to_string();
    }

    if ndigits == 0 {
        if dscale == 0 {
            return "0".to_string();
        }
        return format!("0.{}", "0".repeat(dscale));
    }

    let expected_len = 8 + ndigits * 2;
    if data.len() < expected_len {
        warn!("Numeric data too short for {} digits", ndigits);
        return "0".to_string();
    }

    // Read base-10000 digits
    let mut digits = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let off = 8 + i * 2;
        digits.push(u16::from_be_bytes(data[off..off + 2].try_into().unwrap()));
    }

    let mut result = String::with_capacity(ndigits * 4 + 4);

    if sign == 0x4000 {
        result.push('-');
    }

    let int_digit_count = if weight >= 0 { (weight + 1) as usize } else { 0 };

    // Integer part
    if int_digit_count == 0 {
        result.push('0');
    } else {
        for i in 0..int_digit_count {
            let d = if i < ndigits { digits[i] } else { 0 };
            if i == 0 {
                result.push_str(&d.to_string());
            } else {
                result.push_str(&format!("{:04}", d));
            }
        }
    }

    // Fractional part
    if dscale > 0 {
        result.push('.');
        let mut frac_chars = 0;
        for i in int_digit_count..ndigits {
            let d = digits[i];
            let s = format!("{:04}", d);
            for ch in s.chars() {
                if frac_chars >= dscale {
                    break;
                }
                result.push(ch);
                frac_chars += 1;
            }
        }
        while frac_chars < dscale {
            result.push('0');
            frac_chars += 1;
        }
    }

    result
}

/// Convert tuple values to strings, using relation column types for proper conversion.
pub fn tuple_to_strings(values: &[TupleData], rel: &RelationInfo) -> Vec<String> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let oid = rel.columns.get(i).map(|c| c.type_oid).unwrap_or(0);
            match v {
                TupleData::Text(s) => match oid {
                    // bool — PG sends "t"/"f", ClickHouse UInt8 needs "1"/"0"
                    16 => if s == "t" { "1".to_string() } else { "0".to_string() },
                    // timestamptz — convert to UTC
                    1184 => timestamptz_to_utc(s),
                    _ => s.clone(),
                },
                TupleData::Binary(data) => decode_binary_value(oid, data),
                TupleData::Null | TupleData::Unchanged => "\\N".to_string(),
            }
        })
        .collect()
}
