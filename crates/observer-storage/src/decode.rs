//! Decode one raw OTLP logs WAL frame into partitioned Arrow batches.

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        FixedSizeBinaryBuilder, Int32Builder, StringBuilder, UInt16Builder, UInt32Builder,
        UInt64Builder,
    },
};
use observer_protocol::otlp::{AnyValue, ExportLogsServiceRequest, KeyValue, any_value};
use observer_wal::{Frame, FrameSignal};
use prost::Message;

use crate::{
    CanonicalJsonError, EventHour, SCHEMA_VERSION, SERVICE_NAME_ATTRIBUTE, SPAN_ID_BYTES,
    SPAN_ID_LEN, TRACE_ID_BYTES, TRACE_ID_LEN, canonical_any_value_json, canonical_attributes_json,
    logs_schema,
};

/// Why a logs WAL frame could not be projected into the v1 schema.
#[derive(Debug)]
pub enum DecodeError {
    /// The frame is not a logs frame.
    UnsupportedSignal(FrameSignal),
    /// The payload is not a valid `ExportLogsServiceRequest`, or it contains too many records.
    InvalidPayload(String),
    /// An `AnyValue` is nested deeper than the canonical JSON limit.
    CanonicalJson(CanonicalJsonError),
    /// Arrow rejected a finished batch.
    Arrow(String),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSignal(signal) => {
                write!(formatter, "unsupported WAL signal {signal:?}")
            }
            Self::InvalidPayload(detail) => {
                write!(formatter, "invalid OTLP logs payload: {detail}")
            }
            Self::CanonicalJson(error) => write!(formatter, "invalid OTLP logs payload: {error}"),
            Self::Arrow(detail) => write!(formatter, "arrow record batch: {detail}"),
        }
    }
}

impl Error for DecodeError {}

impl From<CanonicalJsonError> for DecodeError {
    fn from(error: CanonicalJsonError) -> Self {
        Self::CanonicalJson(error)
    }
}

/// Arrow batches produced from one complete logs frame.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedLogs {
    /// WAL sequence of the decoded frame.
    pub sequence: u64,
    /// Tenant stamped on every row.
    pub tenant_id: String,
    /// Receive time copied from the WAL frame.
    pub received_at_unix_nanos: u64,
    /// Log records walked in resource, scope, then record order.
    pub record_count: u32,
    /// Hour partitions ordered by UTC event hour.
    pub partitions: Vec<DecodedPartition>,
}

/// Rows from one frame that share a derived UTC event hour.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedPartition {
    pub hour: EventHour,
    pub batch: RecordBatch,
}

/// Project one logs frame into schema-v1 batches.
///
/// `record_index` follows resource, scope, then record order and is not restarted per hour.
/// `event_time_unix_nano` uses the original event time, then the observed time, then the WAL
/// receive time. OTLP zero means the event or observed time is missing. String bodies are stored
/// unchanged; every other present body is canonical JSON. Invalid or all-zero trace and span ids
/// are null.
pub fn decode_logs_frame(frame: &Frame) -> Result<DecodedLogs, DecodeError> {
    if frame.signal != FrameSignal::Logs {
        return Err(DecodeError::UnsupportedSignal(frame.signal));
    }
    let request = ExportLogsServiceRequest::decode(frame.payload.as_ref())
        .map_err(|error| DecodeError::InvalidPayload(error.to_string()))?;

    let mut record_count = 0u32;
    let mut partitions = BTreeMap::<EventHour, PartitionBuilder>::new();
    for resource_logs in &request.resource_logs {
        let resource_attributes = match &resource_logs.resource {
            Some(resource) => canonical_attributes_json(&resource.attributes)?,
            None => "{}".to_owned(),
        };
        let service_name = resource_logs
            .resource
            .as_ref()
            .and_then(|resource| promoted_service_name(&resource.attributes));
        for scope_logs in &resource_logs.scope_logs {
            let scope_attributes = match &scope_logs.scope {
                Some(scope) => canonical_attributes_json(&scope.attributes)?,
                None => "{}".to_owned(),
            };
            for record in &scope_logs.log_records {
                let record_index = record_count;
                record_count = record_count.checked_add(1).ok_or_else(|| {
                    DecodeError::InvalidPayload("log record index overflowed u32".to_owned())
                })?;
                let time_unix_nano = present_time(record.time_unix_nano);
                let observed_time_unix_nano = present_time(record.observed_time_unix_nano);
                let event_time_unix_nano = time_unix_nano
                    .or(observed_time_unix_nano)
                    .unwrap_or(frame.received_at_unix_nanos);
                let body = body_text(&record.body)?;
                let trace_id = fixed_id::<TRACE_ID_BYTES>(&record.trace_id);
                let span_id = fixed_id::<SPAN_ID_BYTES>(&record.span_id);
                let log_attributes = canonical_attributes_json(&record.attributes)?;
                let severity_text =
                    (!record.severity_text.is_empty()).then_some(record.severity_text.as_str());
                partitions
                    .entry(EventHour::containing(event_time_unix_nano))
                    .or_insert_with(PartitionBuilder::new)
                    .append(&ProjectedRow {
                        frame,
                        record_index,
                        time_unix_nano,
                        observed_time_unix_nano,
                        event_time_unix_nano,
                        severity_number: (record.severity_number != 0)
                            .then_some(record.severity_number),
                        severity_text,
                        body: body.as_deref(),
                        trace_id: trace_id.as_ref().map(|id| id.as_slice()),
                        span_id: span_id.as_ref().map(|id| id.as_slice()),
                        service_name: service_name.as_deref(),
                        resource_attributes: &resource_attributes,
                        scope_attributes: &scope_attributes,
                        log_attributes: &log_attributes,
                    })?;
            }
        }
    }

    let schema = logs_schema();
    let partitions = partitions
        .into_iter()
        .map(|(hour, builder)| {
            builder
                .finish(schema.clone())
                .map(|batch| DecodedPartition { hour, batch })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DecodedLogs {
        sequence: frame.sequence,
        tenant_id: frame.tenant_id.clone(),
        received_at_unix_nanos: frame.received_at_unix_nanos,
        record_count,
        partitions,
    })
}

struct ProjectedRow<'a> {
    frame: &'a Frame,
    record_index: u32,
    time_unix_nano: Option<u64>,
    observed_time_unix_nano: Option<u64>,
    event_time_unix_nano: u64,
    severity_number: Option<i32>,
    severity_text: Option<&'a str>,
    body: Option<&'a str>,
    trace_id: Option<&'a [u8]>,
    span_id: Option<&'a [u8]>,
    service_name: Option<&'a str>,
    resource_attributes: &'a str,
    scope_attributes: &'a str,
    log_attributes: &'a str,
}

struct PartitionBuilder {
    schema_version: UInt16Builder,
    tenant_id: StringBuilder,
    wal_sequence: UInt64Builder,
    record_index: UInt32Builder,
    time_unix_nano: UInt64Builder,
    observed_time_unix_nano: UInt64Builder,
    received_time_unix_nano: UInt64Builder,
    event_time_unix_nano: UInt64Builder,
    severity_number: Int32Builder,
    severity_text: StringBuilder,
    body: StringBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    service_name: StringBuilder,
    resource_attributes: StringBuilder,
    scope_attributes: StringBuilder,
    log_attributes: StringBuilder,
}

impl PartitionBuilder {
    fn new() -> Self {
        Self {
            schema_version: UInt16Builder::new(),
            tenant_id: StringBuilder::new(),
            wal_sequence: UInt64Builder::new(),
            record_index: UInt32Builder::new(),
            time_unix_nano: UInt64Builder::new(),
            observed_time_unix_nano: UInt64Builder::new(),
            received_time_unix_nano: UInt64Builder::new(),
            event_time_unix_nano: UInt64Builder::new(),
            severity_number: Int32Builder::new(),
            severity_text: StringBuilder::new(),
            body: StringBuilder::new(),
            trace_id: FixedSizeBinaryBuilder::new(TRACE_ID_LEN),
            span_id: FixedSizeBinaryBuilder::new(SPAN_ID_LEN),
            service_name: StringBuilder::new(),
            resource_attributes: StringBuilder::new(),
            scope_attributes: StringBuilder::new(),
            log_attributes: StringBuilder::new(),
        }
    }

    fn append(&mut self, row: &ProjectedRow<'_>) -> Result<(), DecodeError> {
        self.schema_version.append_value(SCHEMA_VERSION);
        self.tenant_id.append_value(&row.frame.tenant_id);
        self.wal_sequence.append_value(row.frame.sequence);
        self.record_index.append_value(row.record_index);
        append_optional_u64(&mut self.time_unix_nano, row.time_unix_nano);
        append_optional_u64(
            &mut self.observed_time_unix_nano,
            row.observed_time_unix_nano,
        );
        self.received_time_unix_nano
            .append_value(row.frame.received_at_unix_nanos);
        self.event_time_unix_nano
            .append_value(row.event_time_unix_nano);
        append_optional_i32(&mut self.severity_number, row.severity_number);
        append_optional_str(&mut self.severity_text, row.severity_text);
        append_optional_str(&mut self.body, row.body);
        append_fixed(&mut self.trace_id, row.trace_id)?;
        append_fixed(&mut self.span_id, row.span_id)?;
        append_optional_str(&mut self.service_name, row.service_name);
        self.resource_attributes
            .append_value(row.resource_attributes);
        self.scope_attributes.append_value(row.scope_attributes);
        self.log_attributes.append_value(row.log_attributes);
        Ok(())
    }

    fn finish(mut self, schema: arrow_schema::SchemaRef) -> Result<RecordBatch, DecodeError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.schema_version.finish()),
            Arc::new(self.tenant_id.finish()),
            Arc::new(self.wal_sequence.finish()),
            Arc::new(self.record_index.finish()),
            Arc::new(self.time_unix_nano.finish()),
            Arc::new(self.observed_time_unix_nano.finish()),
            Arc::new(self.received_time_unix_nano.finish()),
            Arc::new(self.event_time_unix_nano.finish()),
            Arc::new(self.severity_number.finish()),
            Arc::new(self.severity_text.finish()),
            Arc::new(self.body.finish()),
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.log_attributes.finish()),
        ];
        RecordBatch::try_new(schema, columns).map_err(|error| DecodeError::Arrow(error.to_string()))
    }
}

fn append_optional_u64(builder: &mut UInt64Builder, value: Option<u64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_str(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_fixed(
    builder: &mut FixedSizeBinaryBuilder,
    value: Option<&[u8]>,
) -> Result<(), DecodeError> {
    match value {
        Some(value) => builder
            .append_value(value)
            .map_err(|error| DecodeError::Arrow(error.to_string())),
        None => {
            builder.append_null();
            Ok(())
        }
    }
}

fn present_time(unix_nano: u64) -> Option<u64> {
    (unix_nano != 0).then_some(unix_nano)
}

fn body_text(body: &Option<AnyValue>) -> Result<Option<String>, DecodeError> {
    let Some(body) = body else {
        return Ok(None);
    };
    match &body.value {
        None | Some(any_value::Value::StringValueStrindex(_)) => Ok(None),
        Some(any_value::Value::StringValue(text)) => Ok(Some(text.clone())),
        Some(_) => Ok(Some(canonical_any_value_json(body)?)),
    }
}

fn promoted_service_name(attributes: &[KeyValue]) -> Option<String> {
    let mut service_name = None;
    for attribute in attributes {
        if attribute.key == SERVICE_NAME_ATTRIBUTE {
            service_name = match attribute
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref())
            {
                Some(any_value::Value::StringValue(text)) => Some(text.clone()),
                _ => None,
            };
        }
    }
    service_name
}

fn fixed_id<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    if bytes.len() != N || bytes.iter().all(|byte| *byte == 0) {
        None
    } else {
        let mut id = [0; N];
        id.copy_from_slice(bytes);
        Some(id)
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, decode_logs_frame};
    use crate::{
        COLUMN_BODY, COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_LOG_ATTRIBUTES,
        COLUMN_OBSERVED_TIME_UNIX_NANO, COLUMN_RECEIVED_TIME_UNIX_NANO, COLUMN_RECORD_INDEX,
        COLUMN_RESOURCE_ATTRIBUTES, COLUMN_SCHEMA_VERSION, COLUMN_SCOPE_ATTRIBUTES,
        COLUMN_SERVICE_NAME, COLUMN_SEVERITY_NUMBER, COLUMN_SEVERITY_TEXT, COLUMN_SPAN_ID,
        COLUMN_TENANT_ID, COLUMN_TIME_UNIX_NANO, COLUMN_TRACE_ID, COLUMN_WAL_SEQUENCE,
        SCHEMA_VERSION, SERVICE_NAME_ATTRIBUTE, logs_schema,
    };
    use arrow_array::{
        Array, FixedSizeBinaryArray, Int32Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
    };
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, InstrumentationScope, KeyValue, KeyValueList,
        LogRecord, Resource, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_wal::{Frame, FrameSignal};
    use proptest::prelude::*;
    use prost::Message;

    const RECEIVED: u64 = 1_704_067_200_000_000_000;
    const HOUR: u64 = 3_600_000_000_000;

    fn any(value: any_value::Value) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    fn string_value(text: &str) -> AnyValue {
        any(any_value::Value::StringValue(text.to_owned()))
    }

    fn attribute(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(value),
            ..Default::default()
        }
    }

    fn record(body: &str) -> LogRecord {
        LogRecord {
            body: Some(string_value(body)),
            ..Default::default()
        }
    }

    fn frame(signal: FrameSignal, payload: impl Into<Bytes>) -> Frame {
        Frame {
            sequence: 7,
            signal,
            received_at_unix_nanos: RECEIVED,
            tenant_id: "tenant-a".to_owned(),
            payload: payload.into(),
        }
    }

    fn logs_frame(request: &ExportLogsServiceRequest) -> Frame {
        frame(FrameSignal::Logs, request.encode_to_vec())
    }

    fn request(resource_logs: Vec<ResourceLogs>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest { resource_logs }
    }

    fn string_column<'a>(batch: &'a arrow_array::RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{name} is not utf8"))
    }

    fn u64_column<'a>(batch: &'a arrow_array::RecordBatch, name: &str) -> &'a UInt64Array {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing {name}"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("{name} is not u64"))
    }

    fn optional_string(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<String> {
        let column = string_column(batch, name);
        (!column.is_null(row)).then(|| column.value(row).to_owned())
    }

    #[test]
    fn empty_and_invalid_payloads() {
        let empty = decode_logs_frame(&frame(FrameSignal::Logs, Bytes::new())).expect("empty");
        assert_eq!(empty.sequence, 7);
        assert_eq!(empty.tenant_id, "tenant-a");
        assert_eq!(empty.received_at_unix_nanos, RECEIVED);
        assert_eq!(empty.record_count, 0);
        assert!(empty.partitions.is_empty());

        let no_records = logs_frame(&request(vec![ResourceLogs {
            scope_logs: vec![ScopeLogs::default()],
            ..Default::default()
        }]));
        let decoded = decode_logs_frame(&no_records).expect("no records");
        assert_eq!(decoded.record_count, 0);
        assert!(decoded.partitions.is_empty());

        let invalid = decode_logs_frame(&frame(FrameSignal::Logs, Bytes::from_static(&[0x80])));
        assert!(matches!(invalid, Err(DecodeError::InvalidPayload(_))));
        let traces = decode_logs_frame(&frame(FrameSignal::Traces, Bytes::new()));
        assert!(matches!(
            traces,
            Err(DecodeError::UnsupportedSignal(FrameSignal::Traces))
        ));
    }

    #[test]
    fn timestamp_fallback_splits_hours_without_restarting_indexes() {
        let decoded = decode_logs_frame(&logs_frame(&request(vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![
                    LogRecord {
                        time_unix_nano: RECEIVED + 2 * HOUR,
                        observed_time_unix_nano: RECEIVED + HOUR,
                        body: Some(string_value("event")),
                        ..Default::default()
                    },
                    LogRecord {
                        observed_time_unix_nano: RECEIVED + HOUR,
                        body: Some(string_value("observed")),
                        ..Default::default()
                    },
                    LogRecord {
                        body: Some(string_value("received")),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }])))
        .expect("decode");

        assert_eq!(decoded.record_count, 3);
        assert_eq!(decoded.partitions.len(), 3);
        let expected = [
            (RECEIVED, 2, None, None, "received"),
            (RECEIVED + HOUR, 1, None, Some(RECEIVED + HOUR), "observed"),
            (
                RECEIVED + 2 * HOUR,
                0,
                Some(RECEIVED + 2 * HOUR),
                Some(RECEIVED + HOUR),
                "event",
            ),
        ];
        for (partition, (hour, index, event, observed, body)) in
            decoded.partitions.iter().zip(expected)
        {
            assert_eq!(partition.hour.start_unix_nano(), hour);
            assert_eq!(partition.batch.schema().as_ref(), logs_schema().as_ref());
            assert_eq!(partition.batch.num_rows(), 1);
            let batch = &partition.batch;
            assert_eq!(u16_at(batch, COLUMN_SCHEMA_VERSION, 0), SCHEMA_VERSION);
            assert_eq!(string_column(batch, COLUMN_TENANT_ID).value(0), "tenant-a");
            assert_eq!(u64_at(batch, COLUMN_WAL_SEQUENCE, 0), 7);
            assert_eq!(u32_at(batch, COLUMN_RECORD_INDEX, 0), index);
            assert_eq!(optional_u64(batch, COLUMN_TIME_UNIX_NANO, 0), event);
            assert_eq!(
                optional_u64(batch, COLUMN_OBSERVED_TIME_UNIX_NANO, 0),
                observed
            );
            assert_eq!(u64_at(batch, COLUMN_RECEIVED_TIME_UNIX_NANO, 0), RECEIVED);
            assert_eq!(u64_at(batch, COLUMN_EVENT_TIME_UNIX_NANO, 0), hour);
            assert_eq!(
                optional_string(batch, COLUMN_BODY, 0).as_deref(),
                Some(body)
            );
            assert_eq!(
                string_column(batch, COLUMN_RESOURCE_ATTRIBUTES).value(0),
                "{}"
            );
            assert_eq!(string_column(batch, COLUMN_SCOPE_ATTRIBUTES).value(0), "{}");
            assert_eq!(string_column(batch, COLUMN_LOG_ATTRIBUTES).value(0), "{}");
        }
    }

    #[test]
    fn same_hour_preserves_source_order_across_resources_and_scopes() {
        let decoded = decode_logs_frame(&logs_frame(&request(vec![
            ResourceLogs {
                scope_logs: vec![
                    ScopeLogs {
                        log_records: vec![record("r0s0a"), record("r0s0b")],
                        ..Default::default()
                    },
                    ScopeLogs {
                        log_records: vec![record("r0s1")],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![record("r1s0")],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ])))
        .expect("decode");

        assert_eq!(decoded.partitions.len(), 1);
        let batch = &decoded.partitions[0].batch;
        let bodies: Vec<_> = (0..4)
            .map(|row| string_column(batch, COLUMN_BODY).value(row).to_owned())
            .collect();
        assert_eq!(bodies, ["r0s0a", "r0s0b", "r0s1", "r1s0"]);
        let indexes: Vec<_> = (0..4)
            .map(|row| u32_at(batch, COLUMN_RECORD_INDEX, row))
            .collect();
        assert_eq!(indexes, [0, 1, 2, 3]);
    }

    #[test]
    fn core_fields_service_promotion_and_ignored_metadata() {
        let trace_id = {
            let mut id = [0_u8; 16];
            id[0] = 1;
            id.to_vec()
        };
        let span_id = {
            let mut id = [0_u8; 8];
            id[7] = 2;
            id.to_vec()
        };
        let decoded = decode_logs_frame(&logs_frame(&request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    attribute("z", any(any_value::Value::IntValue(1))),
                    attribute(SERVICE_NAME_ATTRIBUTE, string_value("old")),
                    attribute("a", any(any_value::Value::BoolValue(true))),
                    attribute(SERVICE_NAME_ATTRIBUTE, string_value("checkout")),
                ],
                dropped_attributes_count: 4,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "ignored-scope".to_owned(),
                    version: "1".to_owned(),
                    attributes: vec![attribute("scope.key", string_value("scope"))],
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    severity_number: 9,
                    severity_text: "INFO".to_owned(),
                    body: Some(any(any_value::Value::KvlistValue(KeyValueList {
                        values: vec![attribute("message", string_value("structured"))],
                    }))),
                    attributes: vec![
                        attribute("b", any(any_value::Value::IntValue(1))),
                        attribute(SERVICE_NAME_ATTRIBUTE, string_value("not-promoted")),
                        attribute("b", any(any_value::Value::IntValue(2))),
                    ],
                    flags: 1,
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    event_name: "ignored.event".to_owned(),
                    ..Default::default()
                }],
                schema_url: "https://example.test/scope".to_owned(),
            }],
            schema_url: "https://example.test/resource".to_owned(),
        }])))
        .expect("decode");

        let batch = &decoded.partitions[0].batch;
        assert_eq!(i32_at(batch, COLUMN_SEVERITY_NUMBER, 0), Some(9));
        assert_eq!(
            optional_string(batch, COLUMN_SEVERITY_TEXT, 0).as_deref(),
            Some("INFO")
        );
        assert_eq!(
            optional_string(batch, COLUMN_BODY, 0).as_deref(),
            Some(r#"{"message":"structured"}"#)
        );
        assert_eq!(
            fixed_at(batch, COLUMN_TRACE_ID, 0),
            Some(trace_id.as_slice())
        );
        assert_eq!(fixed_at(batch, COLUMN_SPAN_ID, 0), Some(span_id.as_slice()));
        assert_eq!(
            optional_string(batch, COLUMN_SERVICE_NAME, 0).as_deref(),
            Some("checkout")
        );
        assert_eq!(
            string_column(batch, COLUMN_RESOURCE_ATTRIBUTES).value(0),
            r#"{"a":true,"service.name":"checkout","z":1}"#
        );
        assert_eq!(
            string_column(batch, COLUMN_SCOPE_ATTRIBUTES).value(0),
            r#"{"scope.key":"scope"}"#
        );
        assert_eq!(
            string_column(batch, COLUMN_LOG_ATTRIBUTES).value(0),
            r#"{"b":2,"service.name":"not-promoted"}"#
        );
        assert!(batch.schema().field_with_name("event_name").is_err());
        assert!(batch.schema().field_with_name("flags").is_err());
    }

    #[test]
    fn invalid_ids_missing_body_and_non_string_service_name_are_null() {
        let decoded = decode_logs_frame(&logs_frame(&request(vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    attribute(SERVICE_NAME_ATTRIBUTE, string_value("api")),
                    attribute(SERVICE_NAME_ATTRIBUTE, any(any_value::Value::IntValue(5))),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    severity_text: String::new(),
                    body: Some(any(any_value::Value::StringValueStrindex(1))),
                    trace_id: vec![0; 16],
                    span_id: vec![1, 2, 3],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }])))
        .expect("decode");

        let batch = &decoded.partitions[0].batch;
        assert!(optional_string(batch, COLUMN_SEVERITY_TEXT, 0).is_none());
        assert!(i32_column(batch).is_null(0));
        assert!(optional_string(batch, COLUMN_BODY, 0).is_none());
        assert!(fixed_at(batch, COLUMN_TRACE_ID, 0).is_none());
        assert!(fixed_at(batch, COLUMN_SPAN_ID, 0).is_none());
        assert!(optional_string(batch, COLUMN_SERVICE_NAME, 0).is_none());
        assert_eq!(
            string_column(batch, COLUMN_RESOURCE_ATTRIBUTES).value(0),
            r#"{"service.name":5}"#
        );
    }

    #[test]
    fn string_body_is_raw_text_and_bytes_body_is_tagged_json() {
        let decoded = decode_logs_frame(&logs_frame(&request(vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![
                    LogRecord {
                        time_unix_nano: RECEIVED,
                        body: Some(string_value("")),
                        ..Default::default()
                    },
                    LogRecord {
                        time_unix_nano: RECEIVED,
                        body: Some(any(any_value::Value::BytesValue(b"hi".to_vec()))),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }])))
        .expect("decode");
        let batch = &decoded.partitions[0].batch;
        assert_eq!(optional_string(batch, COLUMN_BODY, 0).as_deref(), Some(""));
        assert_eq!(
            optional_string(batch, COLUMN_BODY, 1).as_deref(),
            Some(r#"{"$bytes":"aGk="}"#)
        );
    }

    #[test]
    fn decoding_the_same_frame_is_deterministic() {
        let frame = logs_frame(&request(vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![record("one"), record("two")],
                ..Default::default()
            }],
            ..Default::default()
        }]));
        assert_eq!(
            decode_logs_frame(&frame).expect("first"),
            decode_logs_frame(&frame).expect("second")
        );
    }

    proptest! {
        #[test]
        fn record_indexes_follow_source_order(count in 0u32..8) {
            let records = (0..count)
                .map(|index| LogRecord {
                    time_unix_nano: RECEIVED + u64::from(index) * HOUR,
                    body: Some(string_value(&format!("row-{index}"))),
                    ..Default::default()
                })
                .collect();
            let decoded = decode_logs_frame(&logs_frame(&request(vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: records,
                    ..Default::default()
                }],
                ..Default::default()
            }])))
            .unwrap();
            assert_eq!(decoded.record_count, count);
            assert_eq!(decoded.partitions.len(), usize::try_from(count).unwrap());
            for (offset, partition) in decoded.partitions.iter().enumerate() {
                let index = u32::try_from(offset).unwrap();
                assert_eq!(u32_at(&partition.batch, COLUMN_RECORD_INDEX, 0), index);
                assert_eq!(
                    optional_string(&partition.batch, COLUMN_BODY, 0).as_deref(),
                    Some(format!("row-{index}").as_str())
                );
            }
        }
    }

    fn u16_at(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> u16 {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row)
    }

    fn u32_at(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> u32 {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row)
    }

    fn u64_at(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> u64 {
        u64_column(batch, name).value(row)
    }

    fn optional_u64(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<u64> {
        let column = u64_column(batch, name);
        (!column.is_null(row)).then(|| column.value(row))
    }

    fn i32_at(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<i32> {
        let column = i32_column_named(batch, name);
        (!column.is_null(row)).then(|| column.value(row))
    }

    fn i32_column(batch: &arrow_array::RecordBatch) -> &Int32Array {
        i32_column_named(batch, COLUMN_SEVERITY_NUMBER)
    }

    fn i32_column_named<'a>(batch: &'a arrow_array::RecordBatch, name: &str) -> &'a Int32Array {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
    }

    fn fixed_at<'a>(
        batch: &'a arrow_array::RecordBatch,
        name: &str,
        row: usize,
    ) -> Option<&'a [u8]> {
        let column = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        (!column.is_null(row)).then(|| column.value(row))
    }
}
