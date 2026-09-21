use std::{error::Error, fmt, io, path::Path};

use crate::config::ReadinessConfig;

/// Source of filesystem free space for WAL readiness.
pub trait FreeSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64>;
}

/// Production source: `fs2::available_space` of the WAL filesystem.
#[derive(Clone, Copy, Debug, Default)]
pub struct FilesystemFreeSpace;

impl FreeSpace for FilesystemFreeSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64> {
        fs2::available_space(path)
    }
}

/// WAL-volume free-space gate used at startup and on `/ready`.
#[derive(Clone, Debug)]
pub struct Readiness<S> {
    min_free_bytes: u64,
    space: S,
}

#[derive(Debug)]
pub enum ReadinessError {
    InsufficientSpace,
    Query(io::Error),
}

impl fmt::Display for ReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientSpace => {
                formatter.write_str("WAL filesystem free space is below the configured threshold")
            }
            Self::Query(error) => write!(
                formatter,
                "failed to query WAL filesystem free space: {error}"
            ),
        }
    }
}

impl Error for ReadinessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Query(error) => Some(error),
            Self::InsufficientSpace => None,
        }
    }
}

impl Readiness<FilesystemFreeSpace> {
    #[must_use]
    pub fn filesystem(config: ReadinessConfig) -> Self {
        Self::new(config, FilesystemFreeSpace)
    }
}

impl<S> Readiness<S>
where
    S: FreeSpace,
{
    #[must_use]
    pub fn new(config: ReadinessConfig, space: S) -> Self {
        Self {
            min_free_bytes: config.min_free_bytes,
            space,
        }
    }

    /// Fail closed if the WAL filesystem cannot be queried or is below the floor.
    pub fn ensure_startup(&self, wal_directory: &Path) -> Result<(), ReadinessError> {
        match self.available(wal_directory) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ReadinessError::InsufficientSpace),
            Err(error) => Err(ReadinessError::Query(error)),
        }
    }

    /// `serving && !wal_failed && space >= threshold`. Query errors fail closed.
    #[must_use]
    pub fn is_ready(&self, serving: bool, wal_failed: bool, wal_directory: &Path) -> bool {
        serving && !wal_failed && self.available(wal_directory).unwrap_or(false)
    }

    fn available(&self, wal_directory: &Path) -> io::Result<bool> {
        if self.min_free_bytes == 0 {
            return Ok(true);
        }
        Ok(self.space.available_bytes(wal_directory)? >= self.min_free_bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[derive(Clone)]
    struct FixedSpace(Result<u64, io::ErrorKind>);

    impl FreeSpace for FixedSpace {
        fn available_bytes(&self, _path: &Path) -> io::Result<u64> {
            self.0
                .map_err(|kind| io::Error::new(kind, "injected space error"))
        }
    }

    fn readiness(min_free_bytes: u64, space: Result<u64, io::ErrorKind>) -> Readiness<FixedSpace> {
        Readiness::new(ReadinessConfig { min_free_bytes }, FixedSpace(space))
    }

    fn path() -> PathBuf {
        PathBuf::from("/tmp/observer-wal")
    }

    #[test]
    fn startup_passes_when_space_meets_or_exceeds_threshold() {
        let above = readiness(100, Ok(150));
        above.ensure_startup(&path()).expect("above");
        let equal = readiness(100, Ok(100));
        equal.ensure_startup(&path()).expect("equal");
    }

    #[test]
    fn startup_fails_when_space_is_below_threshold_or_query_errors() {
        let below = readiness(100, Ok(99));
        assert!(matches!(
            below.ensure_startup(&path()),
            Err(ReadinessError::InsufficientSpace)
        ));
        let query = readiness(100, Err(io::ErrorKind::Other));
        assert!(matches!(
            query.ensure_startup(&path()),
            Err(ReadinessError::Query(_))
        ));
    }

    #[test]
    fn zero_disables_startup_and_probe_checks() {
        let disabled = readiness(0, Err(io::ErrorKind::Other));
        disabled.ensure_startup(&path()).expect("disabled");
        assert!(disabled.is_ready(true, false, &path()));
    }

    #[test]
    fn ready_truth_table() {
        let cases = [
            (true, false, Ok(100), true),
            (true, false, Ok(99), false),
            (true, false, Ok(101), true),
            (false, false, Ok(100), false),
            (true, true, Ok(100), false),
            (true, false, Err(io::ErrorKind::Other), false),
        ];
        for (serving, wal_failed, space, expected) in cases {
            let gate = readiness(100, space);
            assert_eq!(
                gate.is_ready(serving, wal_failed, &path()),
                expected,
                "serving={serving} wal_failed={wal_failed} space={space:?}"
            );
        }
    }

    #[test]
    fn errors_do_not_include_paths_or_capacity() {
        let rendered = format!("{}", ReadinessError::InsufficientSpace);
        assert!(!rendered.contains("/tmp"));
        assert!(!rendered.contains("99"));
        assert!(!rendered.contains("100"));
    }
}
