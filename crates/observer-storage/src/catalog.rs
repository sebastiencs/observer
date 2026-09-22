//! In-memory index of published commits, ordered by WAL sequence.

use crate::commit::Commit;

/// Published commits for one tenant, oldest range first.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Catalog {
    commits: Vec<Commit>,
}

/// Why a commit cannot join the catalog.
#[derive(Debug, Eq, PartialEq)]
pub enum CatalogError {
    /// The range overlaps a commit that is already published.
    Overlap {
        /// Inclusive first sequence of the rejected commit.
        first_sequence: u64,
        /// Exclusive next sequence of the rejected commit.
        next_sequence: u64,
    },
    /// The range does not continue from the published high-water mark.
    Gap {
        /// Exclusive sequence the catalog expected next.
        expected: u64,
        /// Inclusive first sequence that was offered.
        actual: u64,
    },
    /// The same range was offered with a different fingerprint or file list.
    Conflict {
        /// Inclusive first sequence of the conflicting range.
        first_sequence: u64,
        /// Exclusive next sequence of the conflicting range.
        next_sequence: u64,
    },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overlap {
                first_sequence,
                next_sequence,
            } => write!(
                formatter,
                "commit {first_sequence}-{next_sequence} overlaps a published range"
            ),
            Self::Gap { expected, actual } => write!(
                formatter,
                "commit starting at {actual} does not continue from {expected}"
            ),
            Self::Conflict {
                first_sequence,
                next_sequence,
            } => write!(
                formatter,
                "commit {first_sequence}-{next_sequence} does not match the published descriptor"
            ),
        }
    }
}

impl std::error::Error for CatalogError {}

impl Catalog {
    /// Load commits that must already be contiguous and non-overlapping.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] when the ranges have a gap, overlap, or duplicate.
    pub fn load(mut commits: Vec<Commit>) -> Result<Self, CatalogError> {
        commits.sort_by_key(|commit| (commit.first_sequence, commit.next_sequence));
        let mut catalog = Self {
            commits: Vec::new(),
        };
        for commit in commits {
            catalog.push_new(commit)?;
        }
        Ok(catalog)
    }

    /// Published commits, oldest range first.
    #[must_use]
    pub fn commits(&self) -> &[Commit] {
        &self.commits
    }

    /// Exclusive end of the contiguous published prefix.
    #[must_use]
    pub fn durable_sequence(&self) -> Option<u64> {
        self.commits.last().map(|commit| commit.next_sequence)
    }

    /// Record `commit`, or accept an identical descriptor for the same range.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] when the range overlaps, leaves a gap, or disagrees with an
    /// existing descriptor.
    pub fn insert(&mut self, commit: Commit) -> Result<bool, CatalogError> {
        if let Some(existing) = self.commits.iter().find(|existing| {
            existing.first_sequence == commit.first_sequence
                && existing.next_sequence == commit.next_sequence
        }) {
            if existing.fingerprint != commit.fingerprint
                || existing.rows != commit.rows
                || existing.files != commit.files
            {
                return Err(CatalogError::Conflict {
                    first_sequence: commit.first_sequence,
                    next_sequence: commit.next_sequence,
                });
            }
            return Ok(false);
        }
        self.push_new(commit)?;
        Ok(true)
    }

    fn push_new(&mut self, commit: Commit) -> Result<(), CatalogError> {
        if self.commits.iter().any(|existing| {
            commit.first_sequence < existing.next_sequence
                && existing.first_sequence < commit.next_sequence
        }) {
            return Err(CatalogError::Overlap {
                first_sequence: commit.first_sequence,
                next_sequence: commit.next_sequence,
            });
        }
        if let Some(expected) = self.durable_sequence()
            && commit.first_sequence != expected
        {
            return Err(CatalogError::Gap {
                expected,
                actual: commit.first_sequence,
            });
        }
        self.commits.push(commit);
        Ok(())
    }
}
