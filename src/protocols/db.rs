//! SQL database load-testing engine (relational databases, JDBC-style).
//!
//! JDBC is a Java API with no Rust equivalent, so this engine benchmarks
//! relational databases through the native `sqlx` drivers. The `AnyPool`
//! driver selects the backend from the connection URL scheme:
//!
//! * `postgres://` / `postgresql://`
//! * `mysql://` / `mariadb://`
//! * `sqlite:` / `sqlite://`
//!
//! Each iteration acquires a pooled connection (timing the wait), then executes
//! one query — either `execute` (rows affected) or `fetch` (rows returned,
//! streamed and capped at `db_max_rows`). Pool-acquire latency and query
//! execution latency are reported separately so pool starvation is
//! distinguishable from slow queries.
//!
//! The query text is deliberately never copied into metrics/reports; only the
//! driver, pool size, and fetch mode are surfaced.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use sqlx::any::{AnyPoolOptions, install_default_drivers};
use sqlx::{AnyConnection, AnyPool};
use tokio::sync::OnceCell;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::{DbSample, RequestMetric};
use crate::protocols::ProtocolEngine;

/// Default cap on rows fetched per query (`0` disables the cap).
pub const DEFAULT_MAX_ROWS: u64 = 10_000;

/// Recognized relational database backends (selected by URL scheme).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbDriver {
    Postgres,
    MySql,
    Sqlite,
}

impl DbDriver {
    /// Detect the driver from a connection URL scheme.
    pub fn from_url(url: &str) -> Option<Self> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Some(Self::Postgres)
        } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Some(Self::MySql)
        } else if url.starts_with("sqlite:") {
            Some(Self::Sqlite)
        } else {
            None
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }
}

/// Whether the URL selects the relational-database engine.
pub fn is_db_url(url: &str) -> bool {
    DbDriver::from_url(url).is_some()
}

/// Per-run database constants stamped onto the aggregated report section.
#[derive(Debug, Clone, Default)]
pub struct DbRunInfo {
    pub driver: String,
    pub pool_max: u64,
    pub fetch_mode: String,
}

/// Build per-run info for a DB URL (or `None` for non-DB URLs).
pub fn run_info(url: &str, pool_max: usize, fetch: bool) -> Option<DbRunInfo> {
    DbDriver::from_url(url).map(|driver| DbRunInfo {
        driver: driver.as_str().to_string(),
        pool_max: pool_max as u64,
        fetch_mode: if fetch {
            "fetch".into()
        } else {
            "execute".into()
        },
    })
}

/// SQL database engine backed by a lazily-created `sqlx` connection pool.
pub struct DbEngine {
    url: String,
    driver: DbDriver,
    query: Arc<str>,
    params: Vec<String>,
    fetch: bool,
    max_rows: u64,
    pool_max: usize,
    timeout: Duration,
    chaos: ChaosEngine,
    pool: OnceCell<AnyPool>,
}

impl DbEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &str,
        query: String,
        params: Vec<String>,
        fetch: bool,
        max_rows: u64,
        pool_max: usize,
        chaos: ChaosEngine,
        timeout_secs: u64,
    ) -> Result<Self, String> {
        let driver = DbDriver::from_url(url)
            .ok_or_else(|| format!("unsupported database URL scheme: {url}"))?;
        if query.trim().is_empty() {
            return Err("database URL requires a non-empty db_query".into());
        }
        Ok(Self {
            url: url.to_string(),
            driver,
            query: Arc::from(query),
            params,
            fetch,
            max_rows,
            pool_max: pool_max.max(1),
            timeout: Duration::from_secs(timeout_secs.max(1)),
            chaos,
            pool: OnceCell::new(),
        })
    }

    /// Driver label (used for telemetry and errors).
    pub fn driver(&self) -> &'static str {
        self.driver.as_str()
    }

    /// Lazily create the connection pool on first use.
    ///
    /// A failed attempt leaves the `OnceCell` uninitialized (not poisoned), so
    /// a transient startup outage is retried on the next iteration instead of
    /// permanently disabling the engine.
    async fn pool(&self) -> Result<&AnyPool, sqlx::Error> {
        self.pool
            .get_or_try_init(|| async {
                install_default_drivers();
                AnyPoolOptions::new()
                    .max_connections(self.pool_max as u32)
                    .acquire_timeout(self.timeout)
                    .connect(&self.url)
                    .await
            })
            .await
    }

    /// Apply `CorruptedPayload` by appending an invalid SQL token.
    fn effective_query(&self, chaos_fault: Option<ChaosFault>) -> Arc<str> {
        if let Some(ChaosFault::CorruptedPayload) = chaos_fault {
            Arc::from(format!("{} __strobengine_chaos__", self.query))
        } else {
            Arc::clone(&self.query)
        }
    }

    async fn run_query(
        &self,
        conn: &mut AnyConnection,
        sql: Arc<str>,
        chaos_fault: Option<ChaosFault>,
    ) -> Result<u64, sqlx::Error> {
        // The query is operator-provided (the whole point of a load test), so we
        // assert it safe; dynamic *data* still goes through bind parameters.
        let mut query = sqlx::query::<sqlx::Any>(sqlx::AssertSqlSafe(sql));
        for param in &self.params {
            query = query.bind(param.as_str());
        }
        // MetadataCorruption keeps the SQL unchanged and adds an unmatched bind,
        // so drivers that reject extra parameters surface the fault.
        if let Some(ChaosFault::MetadataCorruption) = chaos_fault {
            query = query.bind("__strobengine_chaos__");
        }
        if self.fetch {
            // Stream rows so a large result set cannot exhaust memory.
            let mut rows: u64 = 0;
            let mut stream = query.fetch(&mut *conn);
            while stream.try_next().await?.is_some() {
                rows += 1;
                if self.max_rows != 0 && rows >= self.max_rows {
                    break;
                }
            }
            Ok(rows)
        } else {
            let result = query.execute(&mut *conn).await?;
            Ok(result.rows_affected())
        }
    }

    async fn execute_with_fault(
        &self,
        start: Instant,
        chaos_fault: Option<ChaosFault>,
    ) -> RequestMetric {
        if let Some(ChaosFault::LatencySpike { duration_ms }) = chaos_fault {
            tracing::trace!(duration_ms, "chaos: latency spike injected");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }
        if let Some(ChaosFault::ConnectionDrop) = chaos_fault {
            tracing::trace!("chaos: connection drop injected");
            return RequestMetric::error(start.elapsed().as_micros(), chaos_fault);
        }

        let sql = self.effective_query(chaos_fault);

        let pool = match self.pool().await {
            Ok(pool) => pool,
            Err(e) => {
                tracing::warn!(driver = self.driver.as_str(), error = %e, "database pool connection failed");
                return self.error_metric(start, chaos_fault, None, None);
            }
        };
        let acquire_start = Instant::now();
        let mut conn = match pool.acquire().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::debug!(error = %e, "database pool acquire failed");
                return self.error_metric(start, chaos_fault, None, Some(pool));
            }
        };
        let acquire_us = acquire_start.elapsed().as_micros() as u64;

        let query_start = Instant::now();
        let result = self.run_query(&mut conn, sql, chaos_fault).await;
        let query_us = query_start.elapsed().as_micros() as u64;
        drop(conn);

        match result {
            Ok(rows) => {
                let sample = DbSample {
                    rows,
                    acquire_us: Some(acquire_us),
                    query_us: Some(query_us),
                    pool_size: pool.size() as u64,
                    pool_idle: pool.num_idle() as u64,
                    ..Default::default()
                };
                RequestMetric::builder(start.elapsed().as_micros(), chaos_fault)
                    .status(200)
                    .db(Some(sample))
                    .build()
            }
            Err(e) => {
                tracing::debug!(driver = self.driver.as_str(), error = %e, "database query failed");
                let sample = DbSample {
                    errors: 1,
                    acquire_us: Some(acquire_us),
                    query_us: Some(query_us),
                    pool_size: pool.size() as u64,
                    pool_idle: pool.num_idle() as u64,
                    ..Default::default()
                };
                RequestMetric::builder(start.elapsed().as_micros(), chaos_fault)
                    .status(0)
                    .db(Some(sample))
                    .build()
            }
        }
    }

    fn error_metric(
        &self,
        start: Instant,
        chaos_fault: Option<ChaosFault>,
        acquire_us: Option<u64>,
        pool: Option<&AnyPool>,
    ) -> RequestMetric {
        let sample = DbSample {
            errors: 1,
            acquire_us,
            pool_size: pool.map(|p| p.size() as u64).unwrap_or(0),
            pool_idle: pool.map(|p| p.num_idle() as u64).unwrap_or(0),
            ..Default::default()
        };
        RequestMetric::builder(start.elapsed().as_micros(), chaos_fault)
            .status(0)
            .db(Some(sample))
            .build()
    }
}

#[async_trait]
impl ProtocolEngine for DbEngine {
    async fn execute_iteration(&self, _target_url: &str) -> RequestMetric {
        let start = Instant::now();
        self.execute_with_fault(start, self.chaos.select_fault())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    fn temp_db_url(tag: &str) -> String {
        let path =
            std::env::temp_dir().join(format!("strobengine-db-{tag}-{}.sqlite", fastrand::u64(..)));
        format!("sqlite://{}?mode=rwc", path.display())
    }

    async fn seed(url: &str) {
        let pool = SqlitePool::connect(url).await.unwrap();
        sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for i in 0..5 {
            sqlx::query("INSERT INTO items (id, name) VALUES (?, ?)")
                .bind(i)
                .bind(format!("item-{i}"))
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
    }

    fn engine(url: &str, query: &str, fetch: bool) -> DbEngine {
        DbEngine::new(
            url,
            query.to_string(),
            Vec::new(),
            fetch,
            0,
            2,
            ChaosEngine::default(),
            5,
        )
        .unwrap()
    }

    #[test]
    fn detects_drivers_from_scheme() {
        assert_eq!(
            DbDriver::from_url("postgres://localhost/db"),
            Some(DbDriver::Postgres)
        );
        assert_eq!(
            DbDriver::from_url("postgresql://localhost/db"),
            Some(DbDriver::Postgres)
        );
        assert_eq!(
            DbDriver::from_url("mysql://localhost/db"),
            Some(DbDriver::MySql)
        );
        assert_eq!(
            DbDriver::from_url("mariadb://localhost/db"),
            Some(DbDriver::MySql)
        );
        assert_eq!(
            DbDriver::from_url("sqlite://data.db"),
            Some(DbDriver::Sqlite)
        );
        assert!(!is_db_url("http://localhost"));
    }

    #[test]
    fn rejects_unsupported_scheme_and_empty_query() {
        assert!(
            DbEngine::new(
                "http://localhost",
                "SELECT 1".into(),
                Vec::new(),
                true,
                0,
                2,
                ChaosEngine::default(),
                5,
            )
            .is_err()
        );
        assert!(
            DbEngine::new(
                "sqlite::memory:",
                "  ".into(),
                Vec::new(),
                true,
                0,
                2,
                ChaosEngine::default(),
                5,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn fetch_counts_rows() {
        let url = temp_db_url("fetch");
        seed(&url).await;
        let eng = engine(&url, "SELECT id FROM items", true);
        let metric = eng.execute_iteration(&url).await;
        assert_eq!(metric.status_code, 200);
        let sample = metric.db.unwrap();
        assert_eq!(sample.rows, 5);
        assert!(sample.acquire_us.is_some());
        assert!(sample.query_us.is_some());
        assert!(sample.pool_size >= 1);
    }

    #[test]
    fn effective_query_reuses_allocation_without_chaos() {
        let eng = engine(&temp_db_url("query"), "SELECT 1", false);
        for fault in [
            None,
            Some(ChaosFault::MetadataCorruption),
            Some(ChaosFault::ConnectionDrop),
            Some(ChaosFault::LatencySpike { duration_ms: 1 }),
        ] {
            assert!(Arc::ptr_eq(&eng.effective_query(fault), &eng.query));
        }
        let corrupted = eng.effective_query(Some(ChaosFault::CorruptedPayload));
        assert_eq!(&*corrupted, "SELECT 1 __strobengine_chaos__");
    }

    #[tokio::test]
    async fn binds_borrowed_params() {
        let url = temp_db_url("params");
        seed(&url).await;
        let eng = DbEngine::new(
            &url,
            "SELECT name FROM items WHERE name = ?".to_string(),
            vec!["item-1".to_string()],
            true,
            0,
            1,
            ChaosEngine::default(),
            5,
        )
        .unwrap();
        let metric = eng.execute_iteration(&url).await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.db.unwrap().rows, 1);
    }

    #[tokio::test]
    async fn metadata_corruption_keeps_sql_and_binds_extra() {
        let url = temp_db_url("metadata");
        seed(&url).await;
        let eng = DbEngine::new(
            &url,
            "SELECT name FROM items WHERE name = ? AND ? IS NOT NULL".to_string(),
            vec!["item-1".to_string()],
            true,
            0,
            1,
            ChaosEngine::default(),
            5,
        )
        .unwrap();
        let sql = eng.effective_query(Some(ChaosFault::MetadataCorruption));
        assert!(Arc::ptr_eq(&sql, &eng.query));
        let metric = eng
            .execute_with_fault(Instant::now(), Some(ChaosFault::MetadataCorruption))
            .await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.db.unwrap().rows, 1);
    }

    #[tokio::test]
    async fn execute_reports_rows_affected() {
        let url = temp_db_url("execute");
        seed(&url).await;
        let eng = engine(&url, "UPDATE items SET name = 'x'", false);
        let metric = eng.execute_iteration(&url).await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.db.unwrap().rows, 5);
    }

    #[tokio::test]
    async fn bad_query_is_an_error() {
        let url = temp_db_url("bad");
        seed(&url).await;
        let eng = engine(&url, "SELECT * FROM does_not_exist", true);
        let metric = eng.execute_iteration(&url).await;
        assert_eq!(metric.status_code, 0);
        assert_eq!(metric.db.unwrap().errors, 1);
    }

    #[tokio::test]
    async fn unreachable_database_is_an_error_and_pool_is_not_poisoned() {
        // Port 1 is not a Postgres server; the pool init fails cleanly.
        let eng = DbEngine::new(
            "postgres://user:pass@127.0.0.1:1/db",
            "SELECT 1".into(),
            Vec::new(),
            true,
            0,
            1,
            ChaosEngine::default(),
            1,
        )
        .unwrap();
        let first = eng.execute_iteration("postgres://127.0.0.1:1/db").await;
        assert_eq!(first.status_code, 0);
        assert_eq!(first.db.unwrap().errors, 1);
        // The cell must remain uninitialized so a later attempt retries.
        assert!(eng.pool.get().is_none());
    }

    #[tokio::test]
    async fn fetch_cap_limits_rows() {
        let url = temp_db_url("cap");
        seed(&url).await;
        let eng = DbEngine::new(
            &url,
            "SELECT id FROM items".to_string(),
            Vec::new(),
            true,
            2,
            1,
            ChaosEngine::default(),
            5,
        )
        .unwrap();
        let metric = eng.execute_iteration(&url).await;
        assert_eq!(metric.db.unwrap().rows, 2);
    }
}
