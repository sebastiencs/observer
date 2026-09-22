//! Arrow schema, OTLP log decoding, and in-memory generations.

mod canonical_json;
mod decode;
mod dynamic;
mod memtable;
mod schema;

pub use canonical_json::{
    CanonicalJsonError, MAX_JSON_DEPTH, canonical_any_value_json, canonical_attributes_json,
};
pub use decode::{DecodeError, DecodedLogs, DecodedPartition, decode_logs_frame};
pub use dynamic::{
    AttributeSource, DynamicColumn, DynamicError, DynamicField, DynamicIdentity, DynamicKind,
    DynamicLimits, DynamicProjection, DynamicSchema, DynamicValue, FIELD_KIND, FIELD_PATH,
    FIELD_SOURCE, discover_dynamic_schema, ordered_field_names, project_dynamic_fields,
    record_dynamic_values,
};
pub use memtable::{
    Appended, Clock, Generation, GenerationPartition, ManualClock, Memtable, MemtableConfig,
    MemtableError, Snapshot, SystemClock,
};
pub use schema::{
    COLUMN_BODY, COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_LOG_ATTRIBUTES,
    COLUMN_OBSERVED_TIME_UNIX_NANO, COLUMN_RECEIVED_TIME_UNIX_NANO, COLUMN_RECORD_INDEX,
    COLUMN_RESOURCE_ATTRIBUTES, COLUMN_SCHEMA_VERSION, COLUMN_SCOPE_ATTRIBUTES,
    COLUMN_SERVICE_NAME, COLUMN_SEVERITY_NUMBER, COLUMN_SEVERITY_TEXT, COLUMN_SPAN_ID,
    COLUMN_TENANT_ID, COLUMN_TIME_UNIX_NANO, COLUMN_TRACE_ID, COLUMN_WAL_SEQUENCE, EventHour,
    SCHEMA_VERSION, SERVICE_NAME_ATTRIBUTE, SPAN_ID_BYTES, SPAN_ID_LEN, TRACE_ID_BYTES,
    TRACE_ID_LEN, logs_batch_schema, logs_schema,
};
