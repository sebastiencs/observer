use std::{
    collections::HashMap,
    error::Error,
    fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use observer_ingest::{AuthConfigError, TokenDirectory};
use serde::Deserialize;

/// File-backed daemon configuration.
#[derive(Clone, Debug)]
pub struct Config {
    pub wal_directory: PathBuf,
    pub listen: ListenConfig,
    pub tokens: TokenDirectory,
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
    listen: ListenConfig,
    tokens: HashMap<String, String>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
    Auth(AuthConfigError),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read config: {error}"),
            Self::Parse(error) => write!(formatter, "invalid config: {error}"),
            Self::Auth(error) => write!(formatter, "invalid token directory: {error}"),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Parse(error) => Some(error),
            Self::Auth(error) => Some(error),
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
        Ok(Self {
            wal_directory: file.wal_directory,
            listen: file.listen,
            tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
wal_directory = "/var/lib/observer/wal"

[listen]
grpc = "0.0.0.0:4317"
http = "0.0.0.0:4318"
admin = "127.0.0.1:8080"

[tokens]
"secret-a" = "tenant-a"
"secret-b" = "tenant-b"
"#;

    #[test]
    fn parses_listen_addresses_and_token_map() {
        let config = Config::parse(SAMPLE).expect("parse");
        assert_eq!(config.wal_directory, PathBuf::from("/var/lib/observer/wal"));
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
    }

    #[test]
    fn rejects_empty_token_table() {
        let error = Config::parse(
            r#"
wal_directory = "/tmp/wal"
[listen]
grpc = "127.0.0.1:4317"
http = "127.0.0.1:4318"
admin = "127.0.0.1:8080"
[tokens]
"#,
        )
        .expect_err("empty tokens");
        assert!(matches!(
            error,
            ConfigError::Auth(AuthConfigError::EmptyDirectory)
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
