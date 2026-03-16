//! Diff-specific YAML configuration.
//!
//! Reuses source/destination from MirrorConfig but adds per-table diff levels.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct DiffConfig {
    pub mirror_name: String,
    pub source: pg2ch_cdc::config::SourceConfig,
    pub destination: pg2ch_cdc::config::DestinationConfig,
    pub tables: Vec<TableDiff>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableDiff {
    pub name: String,
    #[serde(default = "default_level")]
    pub level: DiffLevel,
}

#[derive(Debug, Clone, Deserialize, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum DiffLevel {
    /// Level 1: pg_class.reltuples vs count() without FINAL
    MetadataCount,
    /// Level 2: exact count(*) on both sides
    ExactCount,
    /// Level 3: sorted PK set comparison
    PrimaryKeys,
}

fn default_level() -> DiffLevel {
    DiffLevel::MetadataCount
}

impl DiffConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read diff config: {}", path.display()))?;
        let config: DiffConfig = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse diff config: {}", path.display()))?;
        if config.tables.is_empty() {
            anyhow::bail!("No tables specified in diff config: {}", path.display());
        }
        Ok(config)
    }

    pub fn ch_table_name(&self, table: &str) -> String {
        format!("{}.{}", self.destination.database, table)
    }
}
