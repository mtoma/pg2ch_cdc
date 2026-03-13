//! Thin PostgreSQL client for metadata queries.
//!
//! Two methods: execute(sql) for DDL, query(sql) for SELECTs.
//! All logic lives in SQL strings — no ORM, no abstractions.

use anyhow::{bail, Result};
use tracing::info;

pub struct PgClient {
    conn: libpq::Connection,
}

fn pg_error(r: &libpq::PQResult) -> String {
    r.error_message()
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown error".to_string())
}

impl PgClient {
    pub fn connect(host: &str, port: u16, database: &str, user: &str, password: &str) -> Result<Self> {
        let dsn = format!(
            "host={} port={} dbname={} user={} password={}",
            host, port, database, user, password
        );
        let conn = libpq::Connection::new(&dsn)
            .map_err(|e| anyhow::anyhow!("PG connect failed: {}", e))?;
        if conn.status() != libpq::connection::Status::Ok {
            bail!("PG connect failed: {}", conn.error_message().unwrap_or("unknown"));
        }
        info!("Connected to PostgreSQL {}:{}/{}", host, port, database);
        Ok(Self { conn })
    }

    /// Run a DDL/DML statement (CREATE, ALTER, INSERT, ...). Crashes on error.
    pub fn execute(&self, sql: &str) -> Result<()> {
        let r = self.conn.exec(sql);
        match r.status() {
            libpq::Status::CommandOk | libpq::Status::TuplesOk => Ok(()),
            s => bail!("PG exec failed ({:?}): {}\nSQL: {}", s, pg_error(&r), sql),
        }
    }

    /// Run a SELECT. Returns rows as Vec<Vec<String>>. Crashes on error.
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let r = self.conn.exec(sql);
        match r.status() {
            libpq::Status::TuplesOk => {},
            s => bail!("PG query failed ({:?}): {}\nSQL: {}", s, pg_error(&r), sql),
        }
        let mut rows = Vec::with_capacity(r.ntuples());
        for i in 0..r.ntuples() {
            let mut row = Vec::with_capacity(r.nfields());
            for j in 0..r.nfields() {
                let val = r.value(i, j)
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_default();
                row.push(val);
            }
            rows.push(row);
        }
        Ok(rows)
    }
}
