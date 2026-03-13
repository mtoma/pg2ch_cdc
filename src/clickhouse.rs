//! ClickHouse HTTP client and CDC batch accumulator.
//!
//! Uses raw HTTP POST with `reqwest::blocking` — TabSeparated format,
//! no typed Row structs. Includes TSV escaping and URL encoding.

use anyhow::{bail, Context, Result};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

// ── ClickHouse HTTP client ──────────────────────────────────────────────

pub struct ChClient {
    http: reqwest::blocking::Client,
    base_url: String,
    user: String,
    password: String,
}

impl ChClient {
    pub fn new(host: &str, port: u16, user: &str, password: &str, timeout_secs: u64) -> Self {
        Self {
            http: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout_secs))
                .build()
                .expect("Failed to build HTTP client"),
            base_url: format!("http://{}:{}", host, port),
            user: user.to_string(),
            password: password.to_string(),
        }
    }

    pub fn query(&self, sql: &str) -> Result<String> {
        let sql_preview: String = sql.chars().take(200).collect();
        let resp = self
            .http
            .post(&self.base_url)
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
        let url = format!(
            "{}/?query={}&input_format_tsv_empty_as_default=1",
            self.base_url,
            urlencoding_encode(&query)
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
    rows: Vec<Vec<String>>,
    last_flush: Instant,
    version_counter: u64,
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
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            ch_table,
            columns,
            rows: Vec::new(),
            last_flush: Instant::now(),
            version_counter: now_ms,
            total_applied: 0,
            total_inserts: 0,
            total_updates: 0,
            total_deletes: 0,
            batch_size,
            flush_interval,
        }
    }

    pub fn add_insert(&mut self, values: Vec<String>) {
        self.version_counter += 1;
        self.total_inserts += 1;
        let now = chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.9f")
            .to_string();
        let mut row = values;
        row.push(now);
        row.push("0".into());
        row.push(self.version_counter.to_string());
        self.rows.push(row);
    }

    pub fn add_update(&mut self, values: Vec<String>) {
        self.version_counter += 1;
        self.total_updates += 1;
        let now = chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.9f")
            .to_string();
        let mut row = values;
        row.push(now);
        row.push("0".into());
        row.push(self.version_counter.to_string());
        self.rows.push(row);
    }

    pub fn add_delete(&mut self, values: Vec<String>) {
        self.version_counter += 1;
        self.total_deletes += 1;
        let now = chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.9f")
            .to_string();
        let mut row = values;
        row.push(now);
        row.push("1".into());
        row.push(self.version_counter.to_string());
        self.rows.push(row);
    }

    pub fn should_flush(&self) -> bool {
        self.rows.len() >= self.batch_size
            || (!self.rows.is_empty() && self.last_flush.elapsed() > self.flush_interval)
    }

    pub fn pending_count(&self) -> usize {
        self.rows.len()
    }

    pub fn ch_table_name(&self) -> &str {
        &self.ch_table
    }

    pub fn flush(&mut self, ch: &ChClient) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }

        let all_columns: Vec<&str> = self
            .columns
            .iter()
            .map(|s| s.as_str())
            .chain(["_pg2ch_synced_at", "_pg2ch_is_deleted", "_pg2ch_version"])
            .collect();

        let col_list = all_columns.join(", ");

        // Build TSV payload
        let mut tsv = String::with_capacity(self.rows.len() * 256);
        for row in &self.rows {
            for (i, val) in row.iter().enumerate() {
                if i > 0 {
                    tsv.push('\t');
                }
                tsv_escape_into(&mut tsv, val);
            }
            tsv.push('\n');
        }

        ch.insert_tsv(&self.ch_table, &col_list, &tsv)?;

        let count = self.rows.len();
        self.total_applied += count as u64;
        tracing::debug!(
            "Flushed {} to {} (total: {} — {}I/{}U/{}D)",
            count, self.ch_table, self.total_applied,
            self.total_inserts, self.total_updates, self.total_deletes
        );
        self.rows.clear();
        self.last_flush = Instant::now();
        Ok(())
    }
}

fn tsv_escape_into(buf: &mut String, val: &str) {
    // \N is ClickHouse's TabSeparated NULL marker — must not be escaped
    if val == "\\N" {
        buf.push_str("\\N");
        return;
    }
    for ch in val.chars() {
        match ch {
            '\t' => buf.push_str("\\t"),
            '\n' => buf.push_str("\\n"),
            '\\' => buf.push_str("\\\\"),
            _ => buf.push(ch),
        }
    }
}
