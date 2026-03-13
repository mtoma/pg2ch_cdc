//! pg2ch_cdc — PostgreSQL → ClickHouse CDC mirror.
//!
//! Runs like rsync: inspect source and destination, do what's needed, exit.
//!
//!   pg2ch_cdc --config mirrors/cstat.yaml

mod cdc;
mod clickhouse;
mod config;
mod orchestrator;
mod pg;
mod pgoutput;
mod types;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use config::MirrorConfig;
use orchestrator::run_mirror;

#[derive(Parser)]
#[command(name = "pg2ch_cdc", about = "PostgreSQL → ClickHouse CDC mirror (rsync-like)")]
struct Cli {
    /// YAML config file (e.g. mirrors/cstat.yaml)
    #[arg(long)]
    config: PathBuf,

    /// Plain output (no timestamp/level prefix). Use when running under Airflow.
    #[arg(long)]
    plain: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.plain {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .without_time()
            .with_target(false)
            .with_level(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .init();
    }

    let config = MirrorConfig::load(&cli.config)?;
    run_mirror(&config)
}
