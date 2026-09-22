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
//! `tenants/<tenant>/date=YYYY-MM-DD/hour=HH/<first>-<next>.parquet`. Generations that admitted the same
//! fields share a fingerprint. A later scan groups those generations together, skips a generation
//! whose schema lacks a filtered column, and fills columns that generation lacks with typed nulls.

mod canonical_json;
mod decode;
mod dynamic;
mod layout;
mod memtable;
mod parquet;
mod schema;

pub use canonical_json::{
    CanonicalJsonError, MAX_JSON_DEPTH, canonical_any_value_json, canonical_attributes_json,
};
pub use decode::{DecodeError, DecodedLogs, DecodedPartition, decode_logs_frame};
pub use dynamic::{
    AttributeSource, DynamicColumn, DynamicError, DynamicField, DynamicIdentity, DynamicKind,
    DynamicLimits, DynamicProjection, DynamicSchema, DynamicValue, FIELD_KIND, FIELD_PATH,
    FIELD_SOURCE, discover_dynamic_schema, dynamic_identity, ordered_field_names,
    project_dynamic_fields, record_dynamic_values,
};
pub use layout::{hour_directory, parquet_file_name, parquet_path};
pub use memtable::{
    Appended, Clock, Generation, GenerationPartition, ManualClock, Memtable, MemtableConfig,
    MemtableError, Snapshot, SystemClock, align_batch,
};
pub use parquet::{
    META_FINGERPRINT, META_HOUR_END_UNIX_NANO, META_HOUR_START_UNIX_NANO, META_PROJECTION_VERSION,
    META_ROW_COUNT, META_WAL_FIRST_SEQUENCE, META_WAL_NEXT_SEQUENCE, ParquetError, ParquetFault,
    ParquetFile, ParquetWriteOptions, read_parquet_batches, write_generation,
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
