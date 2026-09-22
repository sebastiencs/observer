use std::{error::Error, fmt, io, path::Path};

use crate::config::ReadinessConfig;

/// Source of filesystem free space for readiness.
pub trait FreeSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64>;
}

/// Production source: `fs2::available_space` of each configured directory.
#[derive(Clone, Copy, Debug, Default)]
pub struct FilesystemFreeSpace;

impl FreeSpace for FilesystemFreeSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64> {
        fs2::available_space(path)
    }
}

/// Filesystem free-space gate used at startup and on `/ready`.
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
                formatter.write_str("filesystem free space is below the configured threshold")
            }
            Self::Query(error) => {
                write!(formatter, "failed to query filesystem free space: {error}")
            }
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

    /// Fail closed if any directory cannot be queried or is below the floor.
    pub fn ensure_startup(&self, directories: &[&Path]) -> Result<(), ReadinessError> {
        match self.available(directories) {
            Ok(true) => Ok(()),
            Ok(false) => Err(ReadinessError::InsufficientSpace),
            Err(error) => Err(ReadinessError::Query(error)),
        }
    }

    /// `serving && !failed && every directory has space >= threshold`. Query errors fail closed.
    #[must_use]
    pub fn is_ready(&self, serving: bool, failed: bool, directories: &[&Path]) -> bool {
        serving && !failed && self.available(directories).unwrap_or(false)
    }

    fn available(&self, directories: &[&Path]) -> io::Result<bool> {
        if self.min_free_bytes == 0 {
            return Ok(true);
        }
        for directory in directories {
            if self.space.available_bytes(directory)? < self.min_free_bytes {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[derive(Clone)]
    struct SplitSpace;

    impl FreeSpace for SplitSpace {
        fn available_bytes(&self, path: &Path) -> io::Result<u64> {
            if path.ends_with("observer-data") {
                Ok(1)
            } else {
                Ok(1_000)
            }
        }
    }

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
        let directory = path();
        let above = readiness(100, Ok(150));
        above.ensure_startup(&[&directory]).expect("above");
        let equal = readiness(100, Ok(100));
        equal.ensure_startup(&[&directory]).expect("equal");
    }

    #[test]
    fn startup_fails_when_space_is_below_threshold_or_query_errors() {
        let directory = path();
        let below = readiness(100, Ok(99));
        assert!(matches!(
            below.ensure_startup(&[&directory]),
            Err(ReadinessError::InsufficientSpace)
        ));
        let query = readiness(100, Err(io::ErrorKind::Other));
        assert!(matches!(
            query.ensure_startup(&[&directory]),
            Err(ReadinessError::Query(_))
        ));
    }

    #[test]
    fn zero_disables_startup_and_probe_checks() {
        let directory = path();
        let disabled = readiness(0, Err(io::ErrorKind::Other));
        disabled.ensure_startup(&[&directory]).expect("disabled");
        assert!(disabled.is_ready(true, false, &[&directory]));
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
            let directory = path();
            let gate = readiness(100, space);
            assert_eq!(
                gate.is_ready(serving, wal_failed, &[&directory]),
                expected,
                "serving={serving} failed={wal_failed} space={space:?}"
            );
        }
    }

    #[test]
    fn either_directory_below_the_floor_fails_startup_and_readiness() {
        let wal = PathBuf::from("/tmp/observer-wal");
        let data = PathBuf::from("/tmp/observer-data");
        let gate = Readiness::new(
            ReadinessConfig {
                min_free_bytes: 100,
            },
            SplitSpace,
        );
        assert!(matches!(
            gate.ensure_startup(&[&wal, &data]),
            Err(ReadinessError::InsufficientSpace)
        ));
        assert!(!gate.is_ready(true, false, &[&wal, &data]));
    }

    #[test]
    fn errors_do_not_include_paths_or_capacity() {
        let rendered = format!("{}", ReadinessError::InsufficientSpace);
        assert!(!rendered.contains("/tmp"));
        assert!(!rendered.contains("99"));
        assert!(!rendered.contains("100"));
    }
}
