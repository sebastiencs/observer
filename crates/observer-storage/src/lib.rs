//! Arrow projection and in-memory generations for OTLP logs.
//!
//! A logs WAL frame stays raw OTLP. Decoding projects it into stable core columns, three canonical
//! JSON fallback columns (`resource_attributes`, `scope_attributes`, and `log_attributes`), and
//! nullable typed columns for admitted attributes. [`core_logs_schema`] is that stable prefix.
//! [`logs_batch_schema`] appends the dynamic fields for one frame or generation.
//!
//! # Names
//!
//! A logical attribute is its source (`resource`, `scope`, or `log`), its original path, and its
//! OTLP type. The physical column name is `{source}_{normalized_path}_{type}`. Normalization
//! lowercases letters and turns every other character into `_`. Type suffixes are `string`,
//! `bool`, `i64`, `f64`, `bytes`, and `json`. Arrow metadata keeps the original identity in
//! `observer.attribute.source`, `observer.attribute.path`, and `observer.attribute.kind`.
//!
//! When different paths normalize to one physical name, every member of that group gains a `__`
//! suffix and a BLAKE3 prefix of the exact path. The prefix grows until the names differ.
//!
//! # Values
//!
//! Nested key-value lists flatten up to the configured depth. An array, or a map past that depth,
//! becomes one canonical JSON value. Duplicate keys keep the last value. Empty values and profiling
//! string indexes produce no dynamic column. `service.name` is copied into `service_name` and is
//! also stored as a resource attribute column and inside `resource_attributes`.
//!
//! # Limits
//!
//! [`decode_logs_frame`] takes a per-frame depth and column cap. A memtable generation then admits
//! new fields until [`MemtableConfig::max_dynamic_columns`], in arrival order. Fields past either
//! cap are omitted from the Arrow batch. Both JSON fallback columns still contain the original
//! attributes.
//!
//! # Generations
//!
//! One active or frozen generation has one union schema and a BLAKE3 fingerprint of that schema.
//! While it is active, individual batches can omit dynamic columns. Freezing reorders every batch
//! to the union schema and fills the gaps with typed nulls. A frozen generation can be written as
//! local Snappy Parquet at
//! `tenants/<tenant>/date=YYYY-MM-DD/hour=HH/<first>-<next>.parquet`. A generation is published by a
//! CRC32C commit descriptor at `tenants/<tenant>/commits/<first>-<next>.commit`. The catalog swaps
//! that descriptor in and the frozen batches out under one lock, so a snapshot sees the rows in
//! memory or in Parquet. A scan reconciles those schemas by physical name and fills columns a
//! generation lacks with typed nulls. Generations that admitted the same
//! fields share a fingerprint. A later filtered scan skips a generation whose schema lacks a
//! filtered column.
//!
//! # Published files
//!
//! Each Parquet file is written in `event_time_unix_nano` descending order, then `wal_sequence`
//! descending order. The commit descriptor stores that order with the file size, row count, and
//! conservative min/max statistics for scalar columns. Binary, JSON, and non-finite floats keep a
//! null count and no bounds. Statistics that fail validation are discarded and the file stays
//! visible. A storage scan still returns rows without imposing that order.
//!
//! # Durability
//!
//! An ingest acknowledgement means the raw OTLP frame is durable in the WAL. It does not mean the
//! row is visible in a storage scan. A row becomes durable storage when every Parquet file for its
//! generation has been synced and the commit descriptor has been renamed into place. The WAL
//! checkpoint moves only after that descriptor is durable. Retention then deletes sealed WAL
//! segments whose exclusive end is at or before the checkpoint; the open segment stays.
//!
//! A crash during Parquet creation, file sync, rename, or directory sync leaves the previous
//! complete generation. A crash after the commit descriptor is durable but before the checkpoint
//! moves exposes the new generation, and restart advances the checkpoint without appending those
//! rows again. A crash while writing the checkpoint leaves a complete old or new checkpoint. A
//! crash during retention never deletes a segment past the checkpoint. Startup fails when the
//! catalog is behind the checkpoint.
//!
//! # Deferred
//!
//! This crate does not start DataFusion, expose an HTTP query API, upload files to object storage,
//! compact Parquet, or promote JSON fallback fields into typed columns automatically.

mod canonical_json;
mod catalog;
mod commit;
mod decode;
mod dynamic;
mod layout;
mod memtable;
mod parquet;
mod recovery;
mod retirement;
mod schema;
mod snapshot;
mod statistics;

pub use canonical_json::{
    CanonicalJsonError, MAX_JSON_DEPTH, canonical_any_value_json, canonical_attributes_json,
};
pub use catalog::{Catalog, CatalogError};
pub use commit::{
    Commit, CommitError, CommitFault, CommitFile, CommitWriteOptions, commit_for, read_commit,
    write_commit,
};
pub use decode::{DecodeError, DecodedLogs, DecodedPartition, decode_logs_frame};
pub use dynamic::{
    AttributeSource, DynamicColumn, DynamicError, DynamicField, DynamicIdentity, DynamicKind,
    DynamicLimits, DynamicProjection, DynamicSchema, DynamicValue, FIELD_KIND, FIELD_PATH,
    FIELD_SOURCE, discover_dynamic_schema, dynamic_identity, ordered_field_names,
    project_dynamic_fields, record_dynamic_values,
};
pub use layout::{
    commit_file_name, commit_path, commits_directory, hour_directory, parquet_file_name,
    parquet_path, retirement_file_name, retirement_path, retirements_directory, tenant_directory,
};
pub use memtable::{
    Appended, Clock, Generation, GenerationPartition, ManualClock, Memtable, MemtableConfig,
    MemtableError, Snapshot, SystemClock, align_batch,
};
pub use parquet::{
    META_FINGERPRINT, META_HOUR_END_UNIX_NANO, META_HOUR_START_UNIX_NANO, META_PROJECTION_VERSION,
    META_ROW_COUNT, META_SORT_ORDER, META_WAL_FIRST_SEQUENCE, META_WAL_NEXT_SEQUENCE, ParquetError,
    ParquetFault, ParquetFile, ParquetWriteOptions, SORT_ORDER_NEWEST_EVENT, read_parquet_batches,
    sort_by_newest_event, write_generation,
};
pub use recovery::{Recovered, RecoveryError, recover};
pub use retirement::{
    RetiredFile, Retirement, RetirementError, RetirementFault, RetirementWriteOptions,
    read_retirement, write_retirement,
};
pub use schema::{
    COLUMN_BODY, COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_LOG_ATTRIBUTES,
    COLUMN_OBSERVED_TIME_UNIX_NANO, COLUMN_RECEIVED_TIME_UNIX_NANO, COLUMN_RECORD_INDEX,
    COLUMN_RESOURCE_ATTRIBUTES, COLUMN_SCHEMA_VERSION, COLUMN_SCOPE_ATTRIBUTES,
    COLUMN_SERVICE_NAME, COLUMN_SEVERITY_NUMBER, COLUMN_SEVERITY_TEXT, COLUMN_SPAN_ID,
    COLUMN_TENANT_ID, COLUMN_TIME_UNIX_NANO, COLUMN_TRACE_ID, COLUMN_WAL_SEQUENCE, EventHour,
    PROJECTION_VERSION, SCHEMA_VERSION, SERVICE_NAME_ATTRIBUTE, SPAN_ID_BYTES, SPAN_ID_LEN,
    TRACE_ID_BYTES, TRACE_ID_LEN, core_logs_schema, logs_batch_schema,
};
pub use snapshot::{
    HourSources, PublishFault, PublishOptions, PublishedFile, Scan, Store, StoreError,
    StoreSnapshot, align_projected,
};
pub use statistics::{
    ColumnStatistics, FileStatistics, StatValue, eligible_for_received_retention, file_statistics,
    statistics_usable,
};
