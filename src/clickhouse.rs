//! ClickHouse HTTP client and CDC batch accumulator.
//!
//! Uses raw HTTP POST with `reqwest::blocking` — TabSeparated format,
//! no typed Row structs. Includes TSV escaping and URL encoding.

use anyhow::{bail, Context, Result};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ── ClickHouse HTTP client ──────────────────────────────────────────────

pub struct ChClient {
    http: reqwest::blocking::Client,
    base_url: String,
    user: String,
    password: String,
    /// Query-string settings appended to every request. Carries
    /// `session_timezone` so ClickHouse resolves naive timestamps against the
    /// mirror's configured timezone rather than the server default, and
    /// `date_time_input_format=best_effort` so a PostgreSQL `timestamptz`
    /// (which arrives with a `+01`-style offset) parses to the right instant.
    settings: String,
}

impl ChClient {
    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        timeout_secs: u64,
        timezone: &str,
    ) -> Self {
        Self {
            http: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout_secs))
                .build()
                .expect("Failed to build HTTP client"),
            base_url: format!("http://{}:{}", host, port),
            user: user.to_string(),
            password: password.to_string(),
            settings: format!(
                "session_timezone={}&date_time_input_format=best_effort",
                urlencoding_encode(timezone)
            ),
        }
    }

    /// `{base_url}/?{settings}` — the endpoint every request goes to.
    fn url(&self) -> String {
        format!("{}/?{}", self.base_url, self.settings)
    }

    pub fn query(&self, sql: &str) -> Result<String> {
        let sql_preview: String = sql.chars().take(200).collect();
        let resp = self
            .http
            .post(self.url())
            .basic_auth(&self.user, Some(&self.password))
            .body(sql.to_string())
            .send()
            .with_context(|| format!("ClickHouse HTTP request failed: {}", sql_preview))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            bail!("ClickHouse error ({}) for [{}]: {}", status, sql_preview, body);
        }
        Ok(resp.text().unwrap_or_default())
    }

    /// Run a query with a one-shot HTTP client using the given timeout.
    /// Use for long-running queries (initial loads via postgresql()) that
    /// can exceed the configured `ch_timeout_secs`.
    pub fn query_with_timeout(&self, sql: &str, timeout_secs: u64) -> Result<String> {
        let sql_preview: String = sql.chars().take(200).collect();
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .context("Failed to build long-timeout HTTP client")?;
        let resp = client
            .post(self.url())
            .basic_auth(&self.user, Some(&self.password))
            .body(sql.to_string())
            .send()
            .with_context(|| format!("ClickHouse HTTP request failed: {}", sql_preview))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            bail!("ClickHouse error ({}) for [{}]: {}", status, sql_preview, body);
        }
        Ok(resp.text().unwrap_or_default())
    }

    pub fn insert_tsv(&self, table: &str, columns: &str, tsv_data: &str) -> Result<()> {
        let query = format!(
            "INSERT INTO {} ({}) FORMAT TabSeparated",
            table, columns
        );
        // No `input_format_tsv_empty_as_default`. With it, an empty field is
        // replaced by the column default — which for a Nullable column is
        // NULL — so a genuine empty string arriving through CDC was silently
        // stored as NULL. We always write every column explicitly, so an empty
        // field means an empty value, never "use the default".
        let url = format!(
            "{}/?query={}&{}",
            self.base_url,
            urlencoding_encode(&query),
            self.settings
        );

        let resp = self
            .http
            .post(&url)
            .basic_auth(&self.user, Some(&self.password))
            .body(tsv_data.to_string())
            .send()
            .with_context(|| format!("ClickHouse insert HTTP request failed: INSERT INTO {} ...", table))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().unwrap_or_default();
            bail!("ClickHouse insert error ({}) for [INSERT INTO {} ...]: {}", status, table, body);
        }
        Ok(())
    }
}

/// Which kind of row is being appended — sets `_pg2ch_is_deleted` and the counter.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RowKind {
    Insert,
    Update,
    Delete,
}

// ── ClickHouse type-string timezone pinning ─────────────────────────────
//
// These post-process a type string ClickHouse itself produced (via DESCRIBE
// TABLE postgresql() or system.columns). We are NOT mapping PG types to CH
// types — ClickHouse still decides Int32 vs Decimal vs String vs DateTime64,
// and we keep no mapping table. We only make the timezone of a DateTime it
// already chose explicit, because DESCRIBE always omits it and an omitted
// timezone silently means "whatever the server default happens to be".

/// Write `tz` into every `DateTime`/`DateTime64` in a ClickHouse type string,
/// leaving anything already carrying a timezone — and every other type —
/// untouched. Handles arbitrary nesting (`Nullable(...)`, `Array(...)`,
/// `Map(...)`, `LowCardinality(...)`).
pub fn pin_datetime_timezone(ch_type: &str, tz: &str) -> String {
    rewrite_datetimes(ch_type, &mut |args: &str| {
        // Already parameterised with a timezone → leave as-is.
        if args.contains('\'') {
            return None;
        }
        Some(if args.trim().is_empty() {
            format!("'{}'", tz)
        } else {
            format!("{}, '{}'", args.trim(), tz)
        })
    })
}

/// Drop the timezone from every `DateTime`/`DateTime64` in a type string.
///
/// Only for COMPARING two type strings. A timezone difference on an existing
/// column is not schema drift: the stored instants are what they are, and
/// `resolve_table_timezone` already treats the column type as the authority.
/// Recreating a table over it would silently convert the mirror's convention
/// as a side effect of a type comparison.
///
/// That is not theoretical. On 2026-09-14, moving type selection off
/// `DESCRIBE postgresql()` and onto `typemap` made 70 columns across 39 tables
/// differ from the configured timezone — `ciq` and `fds` tables still on
/// Europe/Paris from before the UTC migration. Comparing raw would have
/// dropped and reloaded all 39 on their next run: 11.89 billion rows, 85.25
/// GiB, with the slot unable to confirm throughout. The Paris -> UTC migration
/// is real work, but it is scheduled work, not a side effect.
pub fn strip_datetime_timezone(ch_type: &str) -> String {
    rewrite_datetimes(ch_type, &mut |args: &str| {
        match args.find('\'') {
            None => None,
            Some(q) => Some(args[..q].trim_end().trim_end_matches(',').to_string()),
        }
    })
}

/// The timezone declared on the first `DateTime`/`DateTime64` in a type
/// string, or `None` if the type has no DateTime or leaves it unstated.
pub fn datetime_timezone(ch_type: &str) -> Option<String> {
    let mut found = None;
    rewrite_datetimes(ch_type, &mut |args: &str| {
        if found.is_none() {
            if let Some(start) = args.find('\'') {
                if let Some(len) = args[start + 1..].find('\'') {
                    found = Some(args[start + 1..start + 1 + len].to_string());
                }
            }
        }
        None
    });
    found
}

/// True if the type string contains a `DateTime` or `DateTime64` anywhere.
pub fn has_datetime(ch_type: &str) -> bool {
    let mut seen = false;
    rewrite_datetimes(ch_type, &mut |_| {
        seen = true;
        None
    });
    seen
}

/// Walk a ClickHouse type string, invoking `f` with the argument list of each
/// `DateTime`/`DateTime64` found. Returning `Some(new_args)` replaces them.
///
/// Matching is on identifier boundaries so `Decimal(10, 2)`, `Date32` and a
/// column named `my_DateTime_col` are never touched.
fn rewrite_datetimes(ch_type: &str, f: &mut dyn FnMut(&str) -> Option<String>) -> String {
    let bytes = ch_type.as_bytes();
    let mut out = String::with_capacity(ch_type.len() + 16);
    let mut i = 0;

    while i < bytes.len() {
        // Must start at an identifier boundary.
        let boundary = i == 0 || !is_ident_byte(bytes[i - 1]);
        let name_len = if boundary && ch_type[i..].starts_with("DateTime64") {
            Some(10)
        } else if boundary && ch_type[i..].starts_with("DateTime") {
            Some(8)
        } else {
            None
        };

        let Some(name_len) = name_len else {
            out.push(ch_type[i..].chars().next().unwrap());
            i += ch_type[i..].chars().next().unwrap().len_utf8();
            continue;
        };

        // Reject a longer identifier that merely starts with DateTime
        // (e.g. "DateTime64" already handled above, or "DateTimeFoo").
        let after = i + name_len;
        if after < bytes.len() && is_ident_byte(bytes[after]) {
            out.push_str(&ch_type[i..after]);
            i = after;
            continue;
        }

        out.push_str(&ch_type[i..after]);
        i = after;

        // Collect a balanced argument list, if present.
        let args_span = if i < bytes.len() && bytes[i] == b'(' {
            let mut depth = 0usize;
            let mut j = i;
            let mut in_quote = false;
            while j < bytes.len() {
                match bytes[j] {
                    b'\'' => in_quote = !in_quote,
                    b'(' if !in_quote => depth += 1,
                    b')' if !in_quote => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            // Unbalanced → leave the rest of the string alone.
            if depth != 0 { None } else { Some((i + 1, j)) }
        } else {
            None
        };

        match args_span {
            Some((from, to)) => {
                let args = &ch_type[from..to];
                match f(args) {
                    Some(new_args) => out.push_str(&format!("({})", new_args)),
                    None => out.push_str(&ch_type[from - 1..=to]),
                }
                i = to + 1;
            }
            None => {
                // Bare `DateTime` with no argument list.
                if let Some(new_args) = f("") {
                    out.push_str(&format!("({})", new_args));
                }
            }
        }
    }
    out
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn urlencoding_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", b));
            }
        }
    }
    result
}

// ── Batch accumulator ───────────────────────────────────────────────────

pub struct CdcBatch {
    ch_table: String,
    columns: Vec<String>,
    /// Rows are serialised into this buffer as they arrive, not retained as
    /// `Vec<Vec<Option<String>>>` and converted at flush. That cost one String
    /// allocation per field per row — ~1.74M per 96k-row batch of an 18-column
    /// table — plus a second copy of every byte into the payload, and left the
    /// process at 6.2 GB RSS from allocator churn. `clear()` keeps the
    /// capacity, so after the first batch there is no reallocation either.
    tsv: String,
    row_count: usize,
    last_flush: Instant,
    version_counter: u64,
    rel_id: u32,
    pub total_applied: u64,
    pub total_inserts: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
    batch_size: usize,
    flush_interval: Duration,
}

impl CdcBatch {
    pub fn new(
        ch_table: String,
        columns: Vec<String>,
        batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self {
            ch_table,
            columns,
            tsv: String::new(),
            row_count: 0,
            last_flush: Instant::now(),
            version_counter: now_ns,
            rel_id: 0,
            total_applied: 0,
            total_inserts: 0,
            total_updates: 0,
            total_deletes: 0,
            batch_size,
            flush_interval,
        }
    }

    /// Fix the version counter so serialisation can be asserted byte for
    /// byte. Production seeds it from the wall clock.
    #[cfg(test)]
    pub(crate) fn set_version_counter(&mut self, v: u64) {
        self.version_counter = v;
    }

    pub fn set_rel_id(&mut self, id: u32) {
        self.rel_id = id;
    }

    // `_pg2ch_synced_at` is deliberately not sent: the column's
    // `DEFAULT now64()` fills it server-side. Sending a wall clock computed
    // here would be reinterpreted against the column's timezone and land an
    // offset away from the truth, and would put the audit column on a
    // different convention from the data columns beside it.

    /// Append one row, letting the caller write its data fields straight into
    /// the batch buffer.
    ///
    /// The closure receives the buffer positioned at the start of the row and
    /// must write the data columns tab-separated, escaped, with no trailing
    /// tab or newline — `types::write_tuple_into` and
    /// `types::write_delete_row_into` do exactly that. The meta columns and
    /// the row terminator are appended here, so they can never be forgotten.
    pub fn add_row<F: FnOnce(&mut String)>(&mut self, kind: RowKind, write_fields: F) {
        use std::fmt::Write;
        self.version_counter += 1;
        match kind {
            RowKind::Insert => self.total_inserts += 1,
            RowKind::Update => self.total_updates += 1,
            RowKind::Delete => self.total_deletes += 1,
        }
        write_fields(&mut self.tsv);
        let _ = write!(
            self.tsv,
            "\t{}\t{}\t{}\n",
            self.rel_id,
            if matches!(kind, RowKind::Delete) { 1 } else { 0 },
            self.version_counter
        );
        self.row_count += 1;
    }

    pub fn should_flush(&self) -> bool {
        self.row_count >= self.batch_size
            || (self.row_count > 0 && self.last_flush.elapsed() > self.flush_interval)
    }

    pub fn pending_count(&self) -> usize {
        self.row_count
    }

    pub fn ch_table_name(&self) -> &str {
        &self.ch_table
    }

    /// Drop all pending in-memory rows without flushing. Used when a TRUNCATE
    /// arrives for this table — buffered rows from before the truncate are
    /// about to be wiped server-side anyway.
    pub fn discard_pending(&mut self) {
        self.tsv.clear();
        self.row_count = 0;
    }

    /// The column list this batch inserts into, data columns then meta.
    pub fn column_list(&self) -> String {
        self.columns
            .iter()
            .map(|s| s.as_str())
            .chain(["_pg2ch_rel_id", "_pg2ch_is_deleted", "_pg2ch_version"])
            .collect::<Vec<&str>>()
            .join(", ")
    }

    /// Exactly the bytes that would be POSTed to ClickHouse.
    ///
    /// Split out of `flush` so the serialisation — escaping, column order,
    /// the meta columns, the delete marker — can be asserted byte for byte
    /// without a ClickHouse server. This is the CDC hot path: every row that
    /// reaches the mirror is built here, so it is worth being able to pin its
    /// output in a unit test.
    /// The exact bytes that would be POSTed — now simply the buffer.
    pub fn tsv_payload(&self) -> &str {
        &self.tsv
    }

    pub fn flush(&mut self, ch: &ChClient) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }

        let col_list = self.column_list();
        ch.insert_tsv(&self.ch_table, &col_list, &self.tsv)?;

        let count = self.row_count;
        self.total_applied += count as u64;
        tracing::debug!(
            "Flushed {} to {} (total: {} — {}I/{}U/{}D)",
            count, self.ch_table, self.total_applied,
            self.total_inserts, self.total_updates, self.total_deletes
        );
        // clear() keeps the allocation: the next batch reuses it.
        self.tsv.clear();
        self.row_count = 0;
        self.last_flush = Instant::now();
        Ok(())
    }
}

/// Escape a value that is KNOWN not to be NULL.
///
/// Unconditional: a backslash always becomes `\\`, so a genuine value of the
/// two characters `\N` is written `\\N` and read back as those two
/// characters. NULL never reaches here — it is written directly by the
/// serialiser, which knows from `None` rather than guessing from the text.
///
/// This used to short-circuit on `val == "\\N"` and emit it unescaped, on the
/// reasoning that it was our own NULL marker. It could not tell that from a
/// real value, so a text column containing `\N` silently became NULL.
pub(crate) fn tsv_escape_into(buf: &mut String, val: &str) {
    for ch in val.chars() {
        match ch {
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\\' => buf.push_str("\\\\"),
            _ => buf.push(ch),
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_the_types_describe_actually_returns() {
        // These are the exact strings DESCRIBE TABLE postgresql() produces.
        assert_eq!(pin_datetime_timezone("DateTime64(6)", "UTC"), "DateTime64(6, 'UTC')");
        assert_eq!(
            pin_datetime_timezone("Nullable(DateTime64(6))", "UTC"),
            "Nullable(DateTime64(6, 'UTC'))"
        );
        assert_eq!(
            pin_datetime_timezone("Nullable(DateTime64(6))", "Europe/Paris"),
            "Nullable(DateTime64(6, 'Europe/Paris'))"
        );
        // Bare DateTime takes the timezone as its only argument.
        assert_eq!(pin_datetime_timezone("DateTime", "UTC"), "DateTime('UTC')");
        assert_eq!(
            pin_datetime_timezone("Nullable(DateTime)", "UTC"),
            "Nullable(DateTime('UTC'))"
        );
    }

    #[test]
    fn leaves_non_datetime_types_alone() {
        // Decimal's comma must not be mistaken for a timezone slot, and
        // Date/Date32 have no timezone at all.
        for ty in [
            "Int32", "Nullable(Int64)", "String", "Nullable(String)",
            "Decimal(10, 2)", "Nullable(Decimal(38, 9))", "Date", "Date32",
            "Nullable(Date32)", "UUID", "Bool", "Float64",
        ] {
            assert_eq!(pin_datetime_timezone(ty, "UTC"), ty, "type {ty} was modified");
            assert!(!has_datetime(ty), "type {ty} reported as DateTime");
            assert_eq!(datetime_timezone(ty), None);
        }
    }

    #[test]
    fn is_idempotent_and_never_overwrites_an_existing_timezone() {
        // Re-running must not double-pin, and must not silently retype a
        // column that already declares a different convention — the caller
        // needs to see the mismatch and refuse.
        let pinned = "Nullable(DateTime64(6, 'Europe/Paris'))";
        assert_eq!(pin_datetime_timezone(pinned, "Europe/Paris"), pinned);
        assert_eq!(pin_datetime_timezone(pinned, "UTC"), pinned);
        assert_eq!(datetime_timezone(pinned).as_deref(), Some("Europe/Paris"));
        assert_eq!(
            pin_datetime_timezone("DateTime('UTC')", "Europe/Paris"),
            "DateTime('UTC')"
        );
    }

    #[test]
    fn reads_back_the_timezone_it_wrote() {
        for tz in ["UTC", "Europe/Paris", "America/New_York", "Etc/GMT-5"] {
            let pinned = pin_datetime_timezone("Nullable(DateTime64(6))", tz);
            assert_eq!(datetime_timezone(&pinned).as_deref(), Some(tz));
            assert!(has_datetime(&pinned));
        }
    }

    #[test]
    fn handles_nesting_and_identifier_boundaries() {
        assert_eq!(
            pin_datetime_timezone("Array(DateTime64(3))", "UTC"),
            "Array(DateTime64(3, 'UTC'))"
        );
        assert_eq!(
            pin_datetime_timezone("Map(String, DateTime64(6))", "UTC"),
            "Map(String, DateTime64(6, 'UTC'))"
        );
        assert_eq!(
            pin_datetime_timezone("LowCardinality(Nullable(DateTime))", "UTC"),
            "LowCardinality(Nullable(DateTime('UTC')))"
        );
        // A tuple with two DateTimes gets both pinned.
        assert_eq!(
            pin_datetime_timezone("Tuple(DateTime64(6), DateTime)", "UTC"),
            "Tuple(DateTime64(6, 'UTC'), DateTime('UTC'))"
        );
        // "DateTime" appearing inside a longer identifier is not a type.
        assert_eq!(pin_datetime_timezone("MyDateTimeThing", "UTC"), "MyDateTimeThing");
        assert_eq!(pin_datetime_timezone("DateTimeFoo", "UTC"), "DateTimeFoo");
        assert!(!has_datetime("MyDateTimeThing"));
    }

    #[test]
    fn distinguishes_datetime_from_datetime64() {
        // DateTime64 must win the longest-match, or "DateTime" would match
        // first and leave a stray "64(6)" behind.
        assert_eq!(pin_datetime_timezone("DateTime64(9)", "UTC"), "DateTime64(9, 'UTC')");
        assert_eq!(datetime_timezone("DateTime64(9, 'UTC')").as_deref(), Some("UTC"));
    }

    #[test]
    fn config_requires_a_dst_free_timezone() {
        use crate::config::validate_timezone;
        // Missing is an error, and the message has to say what to add.
        let err = validate_timezone("").unwrap_err().to_string();
        assert!(err.contains("defaults to UTC"), "unhelpful message: {err}");
        assert!(validate_timezone(" UTC").is_err());
        assert!(validate_timezone("UTC").is_ok());
        // DST rejection needs ClickHouse's timezone database, so it lives in
        // validate_timezone_in_ch and is covered by tests/test_timezone_dst.sh.
        assert!(validate_timezone("Europe/Paris").is_ok());
    }





    #[test]
    fn strips_the_timezone_for_comparison_only() {
        assert_eq!(strip_datetime_timezone("DateTime64(6, 'UTC')"), "DateTime64(6)");
        assert_eq!(
            strip_datetime_timezone("Nullable(DateTime64(6, 'Europe/Paris'))"),
            "Nullable(DateTime64(6))"
        );
        // Same precision, different zone -> identical once stripped.
        assert_eq!(
            strip_datetime_timezone("DateTime64(6, 'Europe/Paris')"),
            strip_datetime_timezone("DateTime64(6, 'UTC')")
        );
    }

    #[test]
    fn stripping_leaves_a_real_type_change_visible() {
        // A precision change is drift and must survive stripping.
        assert_ne!(
            strip_datetime_timezone("DateTime64(6, 'UTC')"),
            strip_datetime_timezone("DateTime64(3, 'UTC')")
        );
        // So is a widened decimal.
        assert_ne!(
            strip_datetime_timezone("Nullable(Decimal(38, 19))"),
            strip_datetime_timezone("Nullable(Decimal(76, 19))")
        );
        // And a different type entirely.
        assert_ne!(strip_datetime_timezone("Int32"), strip_datetime_timezone("Int64"));
    }

    #[test]
    fn stripping_is_harmless_on_types_without_a_timezone() {
        for t in ["String", "Int32", "Nullable(Decimal(10, 2))", "DateTime64(6)", "Date32"] {
            assert_eq!(strip_datetime_timezone(t), t, "type {t} was modified");
        }
    }

    fn batch(cols: &[&str]) -> CdcBatch {
        let mut b = CdcBatch::new(
            "db.t".to_string(),
            cols.iter().map(|s| s.to_string()).collect(),
            1000,
            Duration::from_secs(60),
        );
        b.set_rel_id(42);
        b.set_version_counter(100);
        b
    }

    #[test]
    fn a_real_backslash_n_is_escaped_and_null_is_not() {
        // The bug this replaced: both were emitted as a bare \N, so a text
        // column containing the two characters \N silently became NULL.
        // Found by tests/test_tsv_escaping.sh on 2026-09-16.
        let mut b = batch(&["v"]);
        b.add_row(RowKind::Insert, |buf| tsv_escape_into(buf, "\\N")); // a real value
        b.add_row(RowKind::Insert, |buf| buf.push_str("\\N"));         // an actual NULL
        assert_eq!(b.tsv_payload(), "\\\\N\t42\t0\t101\n\\N\t42\t0\t102\n");
    }

    #[test]
    fn separators_inside_a_value_cannot_break_the_row() {
        let mut b = batch(&["v"]);
        b.add_row(RowKind::Insert, |buf| tsv_escape_into(buf, "a\tb\nc\\d"));
        // One row, one line: the tab and newline are escaped, not emitted raw.
        let out = b.tsv_payload();
        assert_eq!(out, "a\\tb\\nc\\\\d\t42\t0\t101\n");
        assert_eq!(out.matches('\n').count(), 1, "value broke the row: {out:?}");
        assert_eq!(out.split('\t').count(), 4, "value broke the fields: {out:?}");
    }

    #[test]
    fn an_empty_string_stays_an_empty_field() {
        // Paired with dropping input_format_tsv_empty_as_default: the empty
        // field must reach ClickHouse as an empty string, not the default.
        let mut b = batch(&["v"]);
        b.add_row(RowKind::Insert, |buf| tsv_escape_into(buf, ""));
        assert_eq!(b.tsv_payload(), "\t42\t0\t101\n");
    }

    #[test]
    fn a_delete_marks_the_row_and_keeps_the_key() {
        let mut b = batch(&["k", "v"]);
        b.add_row(RowKind::Delete, |buf| {
            tsv_escape_into(buf, "key1");
            buf.push('\t');
            tsv_escape_into(buf, "");
        });
        // is_deleted = 1, key preserved.
        assert_eq!(b.tsv_payload(), "key1\t\t42\t1\t101\n");
    }

    #[test]
    fn utf8_passes_through_untouched() {
        let mut b = batch(&["v"]);
        b.add_row(RowKind::Insert, |buf| tsv_escape_into(buf, "héllo — 日本語 🚀"));
        assert_eq!(b.tsv_payload(), "héllo — 日本語 🚀\t42\t0\t101\n");
    }

    #[test]
    fn column_list_appends_the_meta_columns_in_order() {
        let b = batch(&["a", "b"]);
        assert_eq!(b.column_list(), "a, b, _pg2ch_rel_id, _pg2ch_is_deleted, _pg2ch_version");
    }
}
