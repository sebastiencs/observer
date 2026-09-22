use std::{
    collections::HashMap,
    error::Error,
    fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use observer_ingest::{AuthConfigError, TokenDirectory};
use serde::Deserialize;

/// Two target WAL segments (256 MiB each).
pub const DEFAULT_MIN_FREE_BYTES: u64 = 512 * 1024 * 1024;

/// File-backed daemon configuration.
#[derive(Clone, Debug)]
pub struct Config {
    pub wal_directory: PathBuf,
    pub data_directory: PathBuf,
    pub listen: ListenConfig,
    pub tokens: TokenDirectory,
    pub readiness: ReadinessConfig,
    pub storage: StorageConfig,
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
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    wal_directory: PathBuf,
    data_directory: PathBuf,
    listen: ListenConfig,
    tokens: HashMap<String, String>,
    readiness: Option<FileReadiness>,
    storage: FileStorage,
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
        })
    }
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
        assert_eq!(
            config.tokens.authenticate(Some("Bearer secret-a")).unwrap(),
            "tenant-a"
        );
        assert_eq!(
            config.tokens.authenticate(Some("Bearer secret-b")).unwrap(),
            "tenant-b"
        );
        assert_eq!(config.readiness.min_free_bytes, DEFAULT_MIN_FREE_BYTES);
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
    fn rejects_zero_frozen_limit() {
        let error = Config::parse(
            r#"
wal_directory = "/tmp/wal"
data_directory = "/tmp/data"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
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
            tokens in prop::collection::hash_map(token(), tenant(), 1..6),
            min_free_bytes in prop::option::of(0_u64..=i64::MAX as u64),
            max_rows in 0_u64..10_000,
            max_bytes in 0_u64..10_000,
            max_age_ms in 0_u64..10_000,
            max_frozen in 1_usize..8,
            max_dynamic_columns in 0_usize..64,
            max_depth in 0_usize..8,
            poll_interval_ms in 0_u64..1_000,
        ) {
            let mut body = format!(
                "wal_directory = \"/tmp/observer-{suffix}\"\ndata_directory = \"/tmp/observer-data-{suffix}\"\n\n[listen]\ngrpc = \"{grpc}\"\nhttp = \"{http}\"\nadmin = \"{admin}\"\n\n[tokens]\n"
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
            prop_assert_eq!(
                config.readiness.min_free_bytes,
                min_free_bytes.unwrap_or(DEFAULT_MIN_FREE_BYTES)
            );
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
