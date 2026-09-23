//! Small OTLP constructors for black-box ingest fixtures.
//!
//! Expected query JSON stays beside each test case. These helpers only build the protobuf request.

use observer_protocol::otlp::{
    AnyValue, ArrayValue, ExportLogsServiceRequest, KeyValue, KeyValueList, LogRecord, Resource,
    ResourceLogs, ScopeLogs, any_value,
};

pub fn logs(resources: Vec<ResourceLogs>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: resources,
    }
}

pub fn resource(attributes: Vec<KeyValue>, scopes: Vec<ScopeLogs>) -> ResourceLogs {
    ResourceLogs {
        resource: Some(Resource {
            attributes,
            ..Default::default()
        }),
        scope_logs: scopes,
        ..Default::default()
    }
}

pub fn scope(attributes: Vec<KeyValue>, records: Vec<LogRecord>) -> ScopeLogs {
    ScopeLogs {
        scope: Some(observer_protocol::otlp::InstrumentationScope {
            attributes,
            ..Default::default()
        }),
        log_records: records,
        ..Default::default()
    }
}

pub fn text_record(body: &str, time_unix_nano: u64) -> LogRecord {
    LogRecord {
        time_unix_nano,
        body: Some(string_value(body)),
        ..Default::default()
    }
}

pub fn attribute(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(value),
        ..Default::default()
    }
}

pub fn string_value(text: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(text.to_owned())),
    }
}

pub fn int_value(value: i64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::IntValue(value)),
    }
}

pub fn bool_value(value: bool) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BoolValue(value)),
    }
}

pub fn double_value(value: f64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::DoubleValue(value)),
    }
}

pub fn bytes_value(value: &[u8]) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BytesValue(value.to_vec())),
    }
}

pub fn array_value(values: Vec<AnyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue { values })),
    }
}

pub fn kv_value(values: Vec<KeyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::KvlistValue(KeyValueList { values })),
    }
}
