//! pg2ch_diff — PostgreSQL ↔ ClickHouse data validation tool.
//!
//! Progressive comparison levels per table: metadata count, exact count, PK comparison.
//!
//!   pg2ch_diff --config diffs/cstat.yaml

mod config;
mod diff;
mod pk_types;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

use config::DiffConfig;
use diff::{run_diff, DiffStatus};

#[derive(Parser)]
#[command(name = "pg2ch_diff", about = "PostgreSQL ↔ ClickHouse data validation")]
struct Cli {
    /// YAML diff config file
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

    let config = DiffConfig::load(&cli.config)?;
    let results = run_diff(&config)?;

    // ── Summary ─────────────────────────────────────────────────────────
    println!();
    tracing::info!("═══ Summary ═══════════════════════════════════════════════════");
    tracing::info!("{:<30} {:>15}  {}", "TABLE", "LEVEL", "RESULT");
    tracing::info!("{}", "─".repeat(70));

    let mut mismatches = 0;
    let mut errors = 0;
    for r in &results {
        let level_str = format!("{:?}", r.level);
        let (status_str, detail) = match &r.status {
            DiffStatus::Match { detail } => ("OK", detail.as_str()),
            DiffStatus::Mismatch { detail } => { mismatches += 1; ("MISMATCH", detail.as_str()) },
            DiffStatus::Error { detail } => { errors += 1; ("ERROR", detail.as_str()) },
        };
        tracing::info!("{:<30} {:>15}  {} — {}", r.table, level_str, status_str, detail);
    }
    tracing::info!("{}", "─".repeat(70));
    tracing::info!(
        "{} tables checked: {} ok, {} mismatches, {} errors",
        results.len(),
        results.len() - mismatches - errors,
        mismatches,
        errors
    );

    if mismatches > 0 || errors > 0 {
        std::process::exit(1);
    }
    Ok(())
}
