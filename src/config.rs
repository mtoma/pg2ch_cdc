//! YAML mirror configuration parsing.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct MirrorConfig {
    pub mirror_name: String,
    pub source: SourceConfig,
    pub destination: DestinationConfig,
    #[serde(default)]
    pub settings: Settings,
    pub tables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub schema: String,
}

#[derive(Debug, Deserialize)]
pub struct DestinationConfig {
    pub host: String,
    #[serde(default = "default_ch_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_parallel_loads")]
    pub parallel_loads: usize,
    #[serde(default)]
    pub binary: bool,
    /// HTTP timeout in seconds for ClickHouse queries (initial load can be slow)
    #[serde(default = "default_ch_timeout")]
    pub ch_timeout_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            parallel_loads: default_parallel_loads(),
            binary: false,
            ch_timeout_secs: default_ch_timeout(),
        }
    }
}

fn default_pg_port() -> u16 { 5432 }
fn default_ch_port() -> u16 { 8123 }
fn default_batch_size() -> usize { 1000 }
fn default_flush_interval() -> u64 { 5 }
fn default_parallel_loads() -> usize { 4 }
fn default_ch_timeout() -> u64 { 3600 }

impl MirrorConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: MirrorConfig = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        if config.tables.is_empty() {
            anyhow::bail!("No tables specified in config file: {}", path.display());
        }

        Ok(config)
    }

    /// Publication name: pg2ch_{mirror_name}
    pub fn publication_name(&self) -> String {
        format!("pg2ch_{}", self.mirror_name)
    }

    /// Slot name: pg2ch_{mirror_name}
    pub fn slot_name(&self) -> String {
        format!("pg2ch_{}", self.mirror_name)
    }

    /// ClickHouse table name for a given source table: {database}.{table}
    pub fn ch_table_name(&self, table: &str) -> String {
        format!("{}.{}", self.destination.database, table)
    }
}
