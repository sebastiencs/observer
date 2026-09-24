use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use observer_ingest::{AuthConfigError, TokenDirectory};
use observer_query::{QueryEngineConfig, QueryOptions};
use serde::Deserialize;

/// Two target WAL segments (256 MiB each).
pub const DEFAULT_MIN_FREE_BYTES: u64 = 512 * 1024 * 1024;
/// Maximum JSON body accepted by `POST /v1/query`.
pub const DEFAULT_QUERY_MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Maximum buffered JSON response produced by `POST /v1/query`.
pub const DEFAULT_QUERY_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Receive-time retention used when `[retention]` is omitted. Zero means retain forever.
pub const DEFAULT_RETENTION_MS: u64 = 604_800_000;
/// How often the janitor applies retention. Zero is rejected.
pub const DEFAULT_JANITOR_INTERVAL_MS: u64 = 3_600_000;
/// How far ahead of receive time a client event timestamp may be.
pub const DEFAULT_MAX_FUTURE_SKEW_MS: u64 = 86_400_000;

/// File-backed daemon configuration.
#[derive(Clone, Debug)]
pub struct Config {
    pub wal_directory: PathBuf,
    pub data_directory: PathBuf,
    pub listen: ListenConfig,
    pub tokens: TokenDirectory,
    pub readiness: ReadinessConfig,
    pub storage: StorageConfig,
    /// Query listener caps and runtime.
    pub query: QueryConfig,
    /// Receive-time retention. `None` durations retain forever.
    pub retention: RetentionConfig,
    /// Future event-time limit applied before a request is appended.
    ///
    /// Read by ingest validation once that check is wired.
    #[allow(dead_code)]
    pub ingest: IngestConfig,
}

/// How long published files are kept, and how often that policy runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionConfig {
    /// Default receive-time retention. `None` retains forever.
    pub default: Option<Duration>,
    /// Delay between janitor passes.
    pub janitor_interval: Duration,
    /// Per-tenant overrides. `None` retains that tenant forever.
    pub tenants: BTreeMap<String, Option<Duration>>,
}

/// Limit on client event timestamps relative to server receive time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestConfig {
    /// A record may be at most this far ahead of the request's receive time.
    pub max_future_skew: Duration,
}

/// HTTP caps and the shared query runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryConfig {
    /// Deadline applied when a request does not set a lower one.
    pub timeout: Duration,
    /// Returned-row cap applied when a request does not set a lower one.
    pub max_rows: usize,
    /// Maximum accepted query request body.
    pub max_request_bytes: usize,
    /// Maximum buffered JSON response.
    pub max_response_bytes: usize,
    /// Runtime settings, including the dedicated spill directory.
    pub engine: QueryEngineConfig,
}

/// Memtable bounds and the consumer poll interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageConfig {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_age: Duration,
    pub max_frozen: usize,
    pub max_dynamic_columns: usize,
    pub max_depth: usize,
    pub poll_interval: Duration,
}

/// Filesystem free-space gate applied to the WAL and data directories. Zero disables the check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessConfig {
    pub min_free_bytes: u64,
}

impl Default for ReadinessConfig {
    fn default() -> Self {
        Self {
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct ListenConfig {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
    pub admin: SocketAddr,
    /// Separate query listener.
    pub query: SocketAddr,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    wal_directory: PathBuf,
    data_directory: PathBuf,
    listen: ListenConfig,
    tokens: HashMap<String, String>,
    readiness: Option<FileReadiness>,
    storage: FileStorage,
    query: Option<FileQuery>,
    retention: Option<FileRetention>,
    ingest: Option<FileIngest>,
}

#[derive(Debug, Deserialize, Default)]
struct FileQuery {
    timeout_ms: Option<u64>,
    max_rows: Option<u64>,
    max_request_bytes: Option<u64>,
    max_response_bytes: Option<u64>,
    memory_pool_bytes: Option<u64>,
    max_concurrent_queries: Option<u64>,
    target_partitions_per_query: Option<u64>,
    batch_size: Option<u64>,
    metadata_cache_bytes: Option<u64>,
    spill_budget_bytes: Option<u64>,
    sort_spill_reservation_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FileStorage {
    max_rows: u64,
    max_bytes: u64,
    max_age_ms: u64,
    max_frozen: usize,
    max_dynamic_columns: usize,
    max_depth: usize,
    poll_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
struct FileReadiness {
    min_free_bytes: u64,
}

#[derive(Debug, Deserialize, Default)]
struct FileRetention {
    default_ms: Option<u64>,
    janitor_interval_ms: Option<u64>,
    tenants: Option<BTreeMap<String, u64>>,
}

#[derive(Debug, Deserialize, Default)]
struct FileIngest {
    max_future_skew_ms: Option<u64>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
    Auth(AuthConfigError),
    Invalid(&'static str),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read config: {error}"),
            Self::Parse(error) => write!(formatter, "invalid config: {error}"),
            Self::Auth(error) => write!(formatter, "invalid token directory: {error}"),
            Self::Invalid(detail) => write!(formatter, "invalid config: {detail}"),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parse(error) => Some(error),
            Self::Auth(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(ConfigError::Io)?;
        Self::parse(&contents)
    }

    pub fn parse(contents: &str) -> Result<Self, ConfigError> {
        let file: FileConfig = toml::from_str(contents).map_err(ConfigError::Parse)?;
        let tokens = TokenDirectory::new(file.tokens).map_err(ConfigError::Auth)?;
        if file.storage.max_frozen == 0 {
            return Err(ConfigError::Invalid("storage.max_frozen must be non-zero"));
        }
        let query = query_config(file.query.unwrap_or_default(), &file.data_directory)?;
        let retention = retention_config(file.retention.unwrap_or_default(), &tokens)?;
        let ingest = ingest_config(file.ingest.unwrap_or_default())?;
        Ok(Self {
            wal_directory: file.wal_directory,
            data_directory: file.data_directory,
            listen: file.listen,
            tokens,
            readiness: file
                .readiness
                .map(|readiness| ReadinessConfig {
                    min_free_bytes: readiness.min_free_bytes,
                })
                .unwrap_or_default(),
            storage: StorageConfig {
                max_rows: file.storage.max_rows,
                max_bytes: file.storage.max_bytes,
                max_age: Duration::from_millis(file.storage.max_age_ms),
                max_frozen: file.storage.max_frozen,
                max_dynamic_columns: file.storage.max_dynamic_columns,
                max_depth: file.storage.max_depth,
                poll_interval: Duration::from_millis(file.storage.poll_interval_ms),
            },
            query,
            retention,
            ingest,
        })
    }
}

fn retention_config(
    file: FileRetention,
    tokens: &TokenDirectory,
) -> Result<RetentionConfig, ConfigError> {
    let default = match file.default_ms.unwrap_or(DEFAULT_RETENTION_MS) {
        0 => None,
        millis => Some(duration_from_millis(
            millis,
            "retention.default_ms is too large",
        )?),
    };
    let janitor_ms = file
        .janitor_interval_ms
        .unwrap_or(DEFAULT_JANITOR_INTERVAL_MS);
    if janitor_ms == 0 {
        return Err(ConfigError::Invalid(
            "retention.janitor_interval_ms must be non-zero",
        ));
    }
    let janitor_interval =
        duration_from_millis(janitor_ms, "retention.janitor_interval_ms is too large")?;
    let known = tokens.tenants().collect::<BTreeSet<_>>();
    let mut tenants = BTreeMap::new();
    for (tenant, millis) in file.tenants.unwrap_or_default() {
        if !known.contains(tenant.as_str()) {
            return Err(ConfigError::Invalid(
                "retention override names an unknown tenant",
            ));
        }
        let duration = match millis {
            0 => None,
            millis => Some(duration_from_millis(
                millis,
                "retention.tenants duration is too large",
            )?),
        };
        tenants.insert(tenant, duration);
    }
    Ok(RetentionConfig {
        default,
        janitor_interval,
        tenants,
    })
}

fn ingest_config(file: FileIngest) -> Result<IngestConfig, ConfigError> {
    Ok(IngestConfig {
        max_future_skew: duration_from_millis(
            file.max_future_skew_ms
                .unwrap_or(DEFAULT_MAX_FUTURE_SKEW_MS),
            "ingest.max_future_skew_ms is too large",
        )?,
    })
}

fn duration_from_millis(millis: u64, too_large: &'static str) -> Result<Duration, ConfigError> {
    let nanos = millis
        .checked_mul(1_000_000)
        .ok_or(ConfigError::Invalid(too_large))?;
    Ok(Duration::new(
        nanos / 1_000_000_000,
        (nanos % 1_000_000_000) as u32,
    ))
}

fn query_config(file: FileQuery, data_directory: &Path) -> Result<QueryConfig, ConfigError> {
    let defaults = QueryEngineConfig::default();
    let engine = QueryEngineConfig {
        memory_pool_bytes: optional_usize(
            file.memory_pool_bytes,
            defaults.memory_pool_bytes,
            "query.memory_pool_bytes must be non-zero",
            "query.memory_pool_bytes is too large",
        )?,
        max_concurrent_queries: optional_usize(
            file.max_concurrent_queries,
            defaults.max_concurrent_queries,
            "query.max_concurrent_queries must be non-zero",
            "query.max_concurrent_queries is too large",
        )?,
        target_partitions_per_query: optional_usize(
            file.target_partitions_per_query,
            defaults.target_partitions_per_query,
            "query.target_partitions_per_query must be non-zero",
            "query.target_partitions_per_query is too large",
        )?,
        batch_size: optional_usize(
            file.batch_size,
            defaults.batch_size,
            "query.batch_size must be non-zero",
            "query.batch_size is too large",
        )?,
        metadata_cache_bytes: optional_usize(
            file.metadata_cache_bytes,
            defaults.metadata_cache_bytes,
            "query.metadata_cache_bytes must be non-zero",
            "query.metadata_cache_bytes is too large",
        )?,
        spill_directory: data_directory.join("query-spill"),
        spill_budget_bytes: match file.spill_budget_bytes {
            None => defaults.spill_budget_bytes,
            Some(0) => {
                return Err(ConfigError::Invalid(
                    "query.spill_budget_bytes must be non-zero",
                ));
            }
            Some(bytes) => bytes,
        },
        sort_spill_reservation_bytes: optional_usize(
            file.sort_spill_reservation_bytes,
            defaults.sort_spill_reservation_bytes,
            "query.sort_spill_reservation_bytes must be non-zero",
            "query.sort_spill_reservation_bytes is too large",
        )?,
    };
    Ok(QueryConfig {
        timeout: match file.timeout_ms {
            None => QueryOptions::TIMEOUT,
            Some(millis) => timeout_from_millis(millis)?,
        },
        max_rows: optional_usize(
            file.max_rows,
            QueryOptions::MAX_ROWS,
            "query.max_rows must be non-zero",
            "query.max_rows is too large",
        )?,
        max_request_bytes: optional_usize(
            file.max_request_bytes,
            DEFAULT_QUERY_MAX_REQUEST_BYTES,
            "query.max_request_bytes must be non-zero",
            "query.max_request_bytes is too large",
        )?,
        max_response_bytes: optional_usize(
            file.max_response_bytes,
            DEFAULT_QUERY_MAX_RESPONSE_BYTES,
            "query.max_response_bytes must be non-zero",
            "query.max_response_bytes is too large",
        )?,
        engine,
    })
}

fn optional_usize(
    value: Option<u64>,
    default: usize,
    zero: &'static str,
    overflow: &'static str,
) -> Result<usize, ConfigError> {
    match value {
        None => Ok(default),
        Some(0) => Err(ConfigError::Invalid(zero)),
        Some(value) => usize::try_from(value).map_err(|_| ConfigError::Invalid(overflow)),
    }
}

fn timeout_from_millis(millis: u64) -> Result<Duration, ConfigError> {
    if millis == 0 {
        return Err(ConfigError::Invalid("query.timeout_ms must be non-zero"));
    }
    let nanos = millis
        .checked_mul(1_000_000)
        .ok_or(ConfigError::Invalid("query.timeout_ms is too large"))?;
    Ok(Duration::new(
        nanos / 1_000_000_000,
        (nanos % 1_000_000_000) as u32,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
wal_directory = "/var/lib/observer/wal"
data_directory = "/var/lib/observer/data"

[listen]
grpc = "0.0.0.0:4317"
http = "0.0.0.0:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"

[tokens]
"secret-a" = "tenant-a"
"secret-b" = "tenant-b"

[storage]
max_rows = 100000
max_bytes = 67108864
max_age_ms = 60000
max_frozen = 4
max_dynamic_columns = 256
max_depth = 4
poll_interval_ms = 50

# Optional. Omitted fields keep these defaults. Spill files use data_directory/query-spill.
# max_concurrent_queries and target_partitions_per_query default to process parallelism.
# [query]
# timeout_ms = 30000
# max_rows = 10000
# max_request_bytes = 65536
# max_response_bytes = 16777216
# memory_pool_bytes = 268435456
# max_concurrent_queries = 4
# target_partitions_per_query = 4
# batch_size = 8192
# metadata_cache_bytes = 33554432
# spill_budget_bytes = 1073741824
# sort_spill_reservation_bytes = 10485760
"#;

    #[test]
    fn parses_listen_addresses_and_token_map() {
        let config = Config::parse(SAMPLE).expect("parse");
        assert_eq!(config.wal_directory, PathBuf::from("/var/lib/observer/wal"));
        assert_eq!(
            config.data_directory,
            PathBuf::from("/var/lib/observer/data")
        );
        assert_eq!(config.storage.max_rows, 100_000);
        assert_eq!(config.storage.max_frozen, 4);
        assert_eq!(config.storage.poll_interval, Duration::from_millis(50));
        assert_eq!(config.listen.grpc, "0.0.0.0:4317".parse().unwrap());
        assert_eq!(config.listen.http, "0.0.0.0:4318".parse().unwrap());
        assert_eq!(config.listen.admin, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.listen.query, "127.0.0.1:8081".parse().unwrap());
        assert_eq!(config.query.timeout, QueryOptions::TIMEOUT);
        assert_eq!(config.query.max_rows, QueryOptions::MAX_ROWS);
        assert_eq!(
            config.query.max_request_bytes,
            DEFAULT_QUERY_MAX_REQUEST_BYTES
        );
        assert_eq!(
            config.query.max_response_bytes,
            DEFAULT_QUERY_MAX_RESPONSE_BYTES
        );
        assert_eq!(
            config.query.engine.spill_directory,
            PathBuf::from("/var/lib/observer/data/query-spill")
        );
        assert_eq!(
            config.query.engine.memory_pool_bytes,
            QueryEngineConfig::default().memory_pool_bytes
        );
        assert_eq!(
            config.tokens.authenticate(Some("Bearer secret-a")).unwrap(),
            "tenant-a"
        );
        assert_eq!(
            config.tokens.authenticate(Some("Bearer secret-b")).unwrap(),
            "tenant-b"
        );
        assert_eq!(config.readiness.min_free_bytes, DEFAULT_MIN_FREE_BYTES);
        assert_eq!(
            config.retention.default,
            Some(Duration::from_millis(DEFAULT_RETENTION_MS))
        );
        assert_eq!(
            config.retention.janitor_interval,
            Duration::from_millis(DEFAULT_JANITOR_INTERVAL_MS)
        );
        assert!(config.retention.tenants.is_empty());
        assert_eq!(
            config.ingest.max_future_skew,
            Duration::from_millis(DEFAULT_MAX_FUTURE_SKEW_MS)
        );
    }

    #[test]
    fn readiness_threshold_can_be_set_or_disabled() {
        let explicit = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
"secret-a" = "tenant-a"
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 1
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
[readiness]
min_free_bytes = 1024
"#,
        )
        .expect("parse");
        assert_eq!(explicit.readiness.min_free_bytes, 1024);

        let disabled = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
"secret-a" = "tenant-a"
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 1
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
[readiness]
min_free_bytes = 0
"#,
        )
        .expect("parse");
        assert_eq!(disabled.readiness.min_free_bytes, 0);
    }

    #[test]
    fn rejects_malformed_min_free_bytes() {
        let error = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
"secret-a" = "tenant-a"
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 1
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
[readiness]
min_free_bytes = "lots"
"#,
        )
        .expect_err("malformed");
        assert!(matches!(error, ConfigError::Parse(_)));
    }

    #[test]
    fn rejects_empty_token_table() {
        let error = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 1
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
"#,
        )
        .expect_err("empty tokens");
        assert!(matches!(
            error,
            ConfigError::Auth(AuthConfigError::EmptyDirectory)
        ));
    }

    #[test]
    fn query_settings_override_the_defaults() {
        let config = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
"secret-a" = "tenant-a"
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 1
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
[query]
timeout_ms = 15000
max_rows = 100
max_request_bytes = 1024
max_response_bytes = 2048
memory_pool_bytes = 4096
max_concurrent_queries = 2
target_partitions_per_query = 3
batch_size = 16
metadata_cache_bytes = 32
spill_budget_bytes = 64
sort_spill_reservation_bytes = 8
"#,
        )
        .expect("parse");
        assert_eq!(config.query.timeout, Duration::from_millis(15_000));
        assert_eq!(config.query.max_rows, 100);
        assert_eq!(config.query.max_request_bytes, 1024);
        assert_eq!(config.query.max_response_bytes, 2048);
        assert_eq!(config.query.engine.memory_pool_bytes, 4096);
        assert_eq!(config.query.engine.max_concurrent_queries, 2);
        assert_eq!(config.query.engine.target_partitions_per_query, 3);
        assert_eq!(config.query.engine.batch_size, 16);
        assert_eq!(config.query.engine.metadata_cache_bytes, 32);
        assert_eq!(config.query.engine.spill_budget_bytes, 64);
        assert_eq!(config.query.engine.sort_spill_reservation_bytes, 8);
        assert_eq!(
            config.query.engine.spill_directory,
            PathBuf::from("/tmp/data/query-spill")
        );
    }

    #[test]
    fn rejects_zero_query_limits() {
        for (field, detail) in [
            ("timeout_ms", "query.timeout_ms must be non-zero"),
            ("max_rows", "query.max_rows must be non-zero"),
            (
                "max_request_bytes",
                "query.max_request_bytes must be non-zero",
            ),
            (
                "max_response_bytes",
                "query.max_response_bytes must be non-zero",
            ),
            (
                "memory_pool_bytes",
                "query.memory_pool_bytes must be non-zero",
            ),
            (
                "max_concurrent_queries",
                "query.max_concurrent_queries must be non-zero",
            ),
            (
                "target_partitions_per_query",
                "query.target_partitions_per_query must be non-zero",
            ),
            ("batch_size", "query.batch_size must be non-zero"),
            (
                "metadata_cache_bytes",
                "query.metadata_cache_bytes must be non-zero",
            ),
            (
                "spill_budget_bytes",
                "query.spill_budget_bytes must be non-zero",
            ),
            (
                "sort_spill_reservation_bytes",
                "query.sort_spill_reservation_bytes must be non-zero",
            ),
        ] {
            let error = Config::parse(&query_config_with(field, 0)).expect_err(field);
            assert!(
                matches!(error, ConfigError::Invalid(message) if message == detail),
                "{field}: {error}"
            );
        }
    }

    #[test]
    fn rejects_a_timeout_that_does_not_fit() {
        let error =
            Config::parse(&query_config_with("timeout_ms", i64::MAX as u64)).expect_err("timeout");
        assert!(matches!(
            error,
            ConfigError::Invalid("query.timeout_ms is too large")
        ));
    }

    fn query_config_with(field: &str, value: u64) -> String {
        format!(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
"secret-a" = "tenant-a"
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 1
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
[query]
{field} = {value}
"#
        )
    }

    #[test]
    fn rejects_zero_frozen_limit() {
        let error = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
query = "127.0.0.1:8081"
[tokens]
"secret-a" = "tenant-a"
[storage]
max_rows = 10
max_bytes = 10
max_age_ms = 1
max_frozen = 0
max_dynamic_columns = 1
max_depth = 1
poll_interval_ms = 1
"#,
        )
        .expect_err("zero frozen");
        assert!(matches!(
            error,
            ConfigError::Invalid("storage.max_frozen must be non-zero")
        ));
    }

    #[test]
    fn debug_does_not_include_tokens() {
        let config = Config::parse(SAMPLE).expect("parse");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("secret-a"));
        assert!(!rendered.contains("secret-b"));
        assert!(rendered.contains("tenant-a"));
    }

    fn with_policy(policy: &str) -> String {
        format!("{SAMPLE}\n{policy}")
    }

    #[test]
    fn retention_overrides_and_zero_means_forever() {
        let config = Config::parse(&with_policy(
            r#"
[retention]
default_ms = 0
janitor_interval_ms = 1000

[retention.tenants]
"tenant-a" = 86400000
"tenant-b" = 0

[ingest]
max_future_skew_ms = 0
"#,
        ))
        .expect("parse");
        assert_eq!(config.retention.default, None);
        assert_eq!(config.retention.janitor_interval, Duration::from_secs(1));
        assert_eq!(
            config.retention.tenants.get("tenant-a").copied(),
            Some(Some(Duration::from_millis(86_400_000)))
        );
        assert_eq!(
            config.retention.tenants.get("tenant-b").copied(),
            Some(None)
        );
        assert_eq!(config.ingest.max_future_skew, Duration::ZERO);
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("secret-a"));
        assert!(!rendered.contains("secret-b"));
    }

    #[test]
    fn rejects_an_unknown_tenant_override_and_a_zero_janitor_interval() {
        let unknown = Config::parse(&with_policy(
            r#"
[retention.tenants]
"tenant-z" = 1
"#,
        ))
        .expect_err("unknown tenant");
        assert!(matches!(
            unknown,
            ConfigError::Invalid("retention override names an unknown tenant")
        ));
        assert!(!unknown.to_string().contains("secret-a"));

        let janitor = Config::parse(&with_policy(
            r#"
[retention]
janitor_interval_ms = 0
"#,
        ))
        .expect_err("zero janitor");
        assert!(matches!(
            janitor,
            ConfigError::Invalid("retention.janitor_interval_ms must be non-zero")
        ));
    }

    #[test]
    fn rejects_retention_and_skew_durations_that_overflow() {
        for (section, detail) in [
            (
                "[retention]\ndefault_ms = 9223372036854775807\n",
                "retention.default_ms is too large",
            ),
            (
                "[retention]\njanitor_interval_ms = 9223372036854775807\n",
                "retention.janitor_interval_ms is too large",
            ),
            (
                "[retention.tenants]\n\"tenant-a\" = 9223372036854775807\n",
                "retention.tenants duration is too large",
            ),
            (
                "[ingest]\nmax_future_skew_ms = 9223372036854775807\n",
                "ingest.max_future_skew_ms is too large",
            ),
        ] {
            let error = Config::parse(&with_policy(section)).expect_err("overflow");
            assert!(
                matches!(error, ConfigError::Invalid(message) if message == detail),
                "{section}: {error}"
            );
            assert!(!error.to_string().contains("secret"));
        }
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::net::SocketAddrV4;

    fn token() -> impl Strategy<Value = String> {
        "tok-[A-Za-z0-9._~-]{1,12}"
    }

    fn tenant() -> impl Strategy<Value = String> {
        "ten-[A-Za-z0-9-]{1,12}"
    }

    proptest! {
        #[test]
        fn parse_round_trips_generated_toml(
            suffix in "[A-Za-z0-9]{1,12}",
            grpc in any::<SocketAddrV4>(),
            http in any::<SocketAddrV4>(),
            admin in any::<SocketAddrV4>(),
            query in any::<SocketAddrV4>(),
            tokens in prop::collection::hash_map(token(), tenant(), 1..6),
            min_free_bytes in prop::option::of(0_u64..=i64::MAX as u64),
            max_rows in 0_u64..10_000,
            max_bytes in 0_u64..10_000,
            max_age_ms in 0_u64..10_000,
            max_frozen in 1_usize..8,
            max_dynamic_columns in 0_usize..64,
            max_depth in 0_usize..8,
            poll_interval_ms in 0_u64..1_000,
            default_ms in prop::option::of(0_u64..10_000),
            janitor_interval_ms in prop::option::of(1_u64..10_000),
            max_future_skew_ms in prop::option::of(0_u64..10_000),
            override_ms in prop::option::of(0_u64..10_000),
        ) {
            let mut body = format!(
                "wal_directory = \"/tmp/observer-{suffix}\"\ndata_directory = \"/tmp/observer-data-{suffix}\"\n\n[listen]\ngrpc = \"{grpc}\"\nhttp = \"{http}\"\nadmin = \"{admin}\"\nquery = \"{query}\"\n\n[tokens]\n"
            );
            for (token, tenant) in &tokens {
                body.push_str(&format!("\"{token}\" = \"{tenant}\"\n"));
            }
            if let Some(min_free_bytes) = min_free_bytes {
                body.push_str(&format!("\n[readiness]\nmin_free_bytes = {min_free_bytes}\n"));
            }
            body.push_str(&format!(
                "\n[storage]\nmax_rows = {max_rows}\nmax_bytes = {max_bytes}\nmax_age_ms = {max_age_ms}\nmax_frozen = {max_frozen}\nmax_dynamic_columns = {max_dynamic_columns}\nmax_depth = {max_depth}\npoll_interval_ms = {poll_interval_ms}\n"
            ));
            if default_ms.is_some() || janitor_interval_ms.is_some() || override_ms.is_some() {
                body.push_str("\n[retention]\n");
                if let Some(default_ms) = default_ms {
                    body.push_str(&format!("default_ms = {default_ms}\n"));
                }
                if let Some(janitor_interval_ms) = janitor_interval_ms {
                    body.push_str(&format!("janitor_interval_ms = {janitor_interval_ms}\n"));
                }
                if let Some(override_ms) = override_ms {
                    let tenant = tokens.values().next().expect("tenant");
                    body.push_str(&format!(
                        "\n[retention.tenants]\n\"{tenant}\" = {override_ms}\n"
                    ));
                }
            }
            if let Some(max_future_skew_ms) = max_future_skew_ms {
                body.push_str(&format!(
                    "\n[ingest]\nmax_future_skew_ms = {max_future_skew_ms}\n"
                ));
            }

            let config = Config::parse(&body).expect("parse");
            prop_assert_eq!(
                config.wal_directory,
                PathBuf::from(format!("/tmp/observer-{suffix}"))
            );
            prop_assert_eq!(
                config.data_directory,
                PathBuf::from(format!("/tmp/observer-data-{suffix}"))
            );
            prop_assert_eq!(config.storage.max_rows, max_rows);
            prop_assert_eq!(config.storage.max_frozen, max_frozen);
            prop_assert_eq!(config.storage.poll_interval, Duration::from_millis(poll_interval_ms));
            prop_assert_eq!(config.listen.grpc, SocketAddr::V4(grpc));
            prop_assert_eq!(config.listen.http, SocketAddr::V4(http));
            prop_assert_eq!(config.listen.admin, SocketAddr::V4(admin));
            prop_assert_eq!(config.listen.query, SocketAddr::V4(query));
            prop_assert_eq!(config.query.timeout, QueryOptions::TIMEOUT);
            prop_assert_eq!(config.query.max_rows, QueryOptions::MAX_ROWS);
            prop_assert_eq!(
                config.query.engine.spill_directory,
                PathBuf::from(format!("/tmp/observer-data-{suffix}/query-spill"))
            );
            prop_assert_eq!(
                config.readiness.min_free_bytes,
                min_free_bytes.unwrap_or(DEFAULT_MIN_FREE_BYTES)
            );
            let expected_default = match default_ms.unwrap_or(DEFAULT_RETENTION_MS) {
                0 => None,
                millis => Some(Duration::from_millis(millis)),
            };
            prop_assert_eq!(config.retention.default, expected_default);
            prop_assert_eq!(
                config.retention.janitor_interval,
                Duration::from_millis(janitor_interval_ms.unwrap_or(DEFAULT_JANITOR_INTERVAL_MS))
            );
            prop_assert_eq!(
                config.ingest.max_future_skew,
                Duration::from_millis(max_future_skew_ms.unwrap_or(DEFAULT_MAX_FUTURE_SKEW_MS))
            );
            if let Some(override_ms) = override_ms {
                let tenant = tokens.values().next().expect("tenant");
                let expected = match override_ms {
                    0 => None,
                    millis => Some(Duration::from_millis(millis)),
                };
                prop_assert_eq!(
                    config.retention.tenants.get(tenant).copied(),
                    Some(expected)
                );
            } else {
                prop_assert!(config.retention.tenants.is_empty());
            }
            for (token, tenant) in &tokens {
                let header = format!("Bearer {token}");
                let resolved = config
                    .tokens
                    .authenticate(Some(&header))
                    .expect("bound token");
                prop_assert_eq!(resolved, tenant.as_str());
            }
        }
    }
}
