//! Local directory layout for one tenant's hour-partitioned Parquet files.
//!
//! `data_directory/tenants/<tenant>/date=YYYY-MM-DD/hour=HH/<first>-<next>.parquet`
//!
//! Commit descriptors live at `tenants/<tenant>/commits/<first>-<next>.commit`. Retirement
//! descriptors live at `tenants/<tenant>/retirements/<first>-<next>.retire`. `<first>-<next>` is
//! the generation's exclusive WAL sequence range. Replaying that range writes the same paths.

use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

use crate::EventHour;

const TENANTS_DIR: &str = "tenants";
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// `tenants/<tenant>` under the data directory.
#[must_use]
pub fn tenant_directory(root: &Path, tenant: &str) -> PathBuf {
    root.join(TENANTS_DIR).join(tenant)
}

/// Directory of commit descriptors for one tenant.
#[must_use]
pub fn commits_directory(root: &Path, tenant: &str) -> PathBuf {
    tenant_directory(root, tenant).join("commits")
}

/// File name for one generation's commit descriptor.
#[must_use]
pub fn commit_file_name(first_sequence: u64, next_sequence: u64) -> String {
    format!("{first_sequence}-{next_sequence}.commit")
}

/// Full path of one generation's commit descriptor.
#[must_use]
pub fn commit_path(root: &Path, tenant: &str, first_sequence: u64, next_sequence: u64) -> PathBuf {
    commits_directory(root, tenant).join(commit_file_name(first_sequence, next_sequence))
}

/// Directory of retirement descriptors for one tenant.
#[must_use]
pub fn retirements_directory(root: &Path, tenant: &str) -> PathBuf {
    tenant_directory(root, tenant).join("retirements")
}

/// File name for one generation's retirement descriptor.
#[must_use]
pub fn retirement_file_name(first_sequence: u64, next_sequence: u64) -> String {
    format!("{first_sequence}-{next_sequence}.retire")
}

/// Full path of one generation's retirement descriptor.
#[must_use]
pub fn retirement_path(
    root: &Path,
    tenant: &str,
    first_sequence: u64,
    next_sequence: u64,
) -> PathBuf {
    retirements_directory(root, tenant).join(retirement_file_name(first_sequence, next_sequence))
}

/// Directory that holds every Parquet file for one tenant hour.
#[must_use]
pub fn hour_directory(root: &Path, tenant: &str, hour: EventHour) -> PathBuf {
    let (year, month, day) = hour.utc_date();
    tenant_directory(root, tenant)
        .join(format!("date={year:04}-{month:02}-{day:02}"))
        .join(format!("hour={:02}", hour.utc_hour()))
}

/// File name for one generation inside an hour directory.
#[must_use]
pub fn parquet_file_name(first_sequence: u64, next_sequence: u64) -> String {
    format!("{first_sequence}-{next_sequence}.parquet")
}

/// Full path of one hour file for a generation.
#[must_use]
pub fn parquet_path(
    root: &Path,
    tenant: &str,
    hour: EventHour,
    first_sequence: u64,
    next_sequence: u64,
) -> PathBuf {
    hour_directory(root, tenant, hour).join(parquet_file_name(first_sequence, next_sequence))
}

/// Exclusive end of the UTC hour that contains `hour`.
#[must_use]
pub fn hour_end_unix_nano(hour: EventHour) -> u64 {
    hour.start_unix_nano().saturating_add(NANOS_PER_HOUR)
}

/// Fsync `start` and every parent up through `root`.
pub(crate) fn sync_ancestors(start: &Path, root: &Path) -> io::Result<()> {
    if !start.starts_with(root) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is outside the data directory",
        ));
    }
    let mut current = start.to_path_buf();
    loop {
        File::open(&current)?.sync_all()?;
        if current == root {
            return Ok(());
        }
        current = current.parent().map(Path::to_path_buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "directory has no parent")
        })?;
    }
}
