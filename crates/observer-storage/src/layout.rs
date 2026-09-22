//! Local directory layout for one tenant's hour-partitioned Parquet files.
//!
//! `data_directory/tenants/<tenant>/date=YYYY-MM-DD/hour=HH/<first>-<next>.parquet`
//!
//! `<first>-<next>` is the generation's exclusive WAL sequence range. Replaying that range writes
//! the same path.

use std::path::{Path, PathBuf};

use crate::EventHour;

const TENANTS_DIR: &str = "tenants";
const NANOS_PER_HOUR: u64 = 3_600_000_000_000;

/// Directory that holds every Parquet file for one tenant hour.
#[must_use]
pub fn hour_directory(root: &Path, tenant: &str, hour: EventHour) -> PathBuf {
    let (year, month, day) = hour.utc_date();
    root.join(TENANTS_DIR)
        .join(tenant)
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
