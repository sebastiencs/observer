//! Buffered JSON for one query result.
//!
//! Values keep the exact Arrow datum: 64-bit integers, decimals, and temporal values are decimal
//! strings, binary values are standard base64, and smaller integers and finite floats are JSON
//! numbers. Nulls and non-finite floats are JSON null. Nested lists, structs, and maps stay
//! nested. A map is a JSON array of `{ "key", "value" }` objects so key types and order survive.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DictionaryArray,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, RecordBatch, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use observer_query::QueryMetrics;
use serde::{Deserialize, Serialize};

/// Stable `error.code` when a result column cannot be represented.
pub const UNSUPPORTED_RESULT_TYPE: &str = "unsupported_result_type";
/// Stable `error.code` when the buffered JSON would exceed the response cap.
pub const RESPONSE_TOO_LARGE: &str = "response_too_large";

/// `POST /v1/query` body. Unknown fields are rejected.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// Single SQL statement. The engine accepts one `SELECT`.
    pub sql: String,
    /// Client timeout in milliseconds. The server cap still applies.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Client row cap. The server cap still applies.
    #[serde(default)]
    pub max_rows: Option<u64>,
}

/// One result column, in query order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaField {
    /// Output name, including SQL aliases.
    pub name: String,
    /// Arrow type, including nested field nullability.
    pub data_type: String,
    /// Whether the column itself accepts nulls.
    pub nullable: bool,
}

/// [`QueryMetrics`] rendered with exact integer counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryMetricsBody {
    /// Admission wait in nanoseconds.
    pub admission_wait_ns: u128,
    /// Planning time in nanoseconds.
    pub planning_ns: u128,
    /// Execution time in nanoseconds.
    pub execution_ns: u128,
    /// Parquet files included in the scan.
    pub files_scanned: u64,
    /// Parquet files dropped before the scan.
    pub files_pruned: u64,
    /// Parquet row groups DataFusion skipped with statistics.
    pub row_groups_pruned: u64,
    /// Rows the stream yielded.
    pub rows_returned: u64,
    /// High-water mark of the shared memory pool, in bytes.
    pub memory_peak_bytes: u64,
    /// Bytes DataFusion operators wrote to spill files.
    pub spill_bytes: u64,
    /// The query hit its deadline.
    pub timed_out: bool,
    /// The caller cancelled the stream or dropped it before it finished.
    pub cancelled: bool,
}

impl QueryMetricsBody {
    fn from_metrics(metrics: QueryMetrics) -> Self {
        Self {
            admission_wait_ns: duration_nanos(metrics.admission_wait),
            planning_ns: duration_nanos(metrics.planning),
            execution_ns: duration_nanos(metrics.execution),
            files_scanned: metrics.files_scanned,
            files_pruned: metrics.files_pruned,
            row_groups_pruned: metrics.row_groups_pruned,
            rows_returned: metrics.rows_returned,
            memory_peak_bytes: metrics.memory_peak_bytes,
            spill_bytes: metrics.spill_bytes,
            timed_out: metrics.timed_out,
            cancelled: metrics.cancelled,
        }
    }
}

/// JSON body returned for a failed query.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct QueryErrorBody {
    /// Stable machine-readable code.
    pub code: &'static str,
    /// Arrow type that could not be encoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryErrorResponse<'a> {
    error: &'a QueryErrorBody,
}

/// Why a result could not be encoded into one JSON document.
#[derive(Debug, PartialEq, Eq)]
pub enum EncodeError {
    /// The result contains an Arrow type this encoder does not represent.
    UnsupportedResultType {
        /// Short Arrow type name.
        data_type: String,
    },
    /// The document would grow past the configured response cap.
    ResponseTooLarge {
        /// Configured maximum, in bytes.
        max_bytes: usize,
    },
}

impl EncodeError {
    /// Structured error body for this failure.
    #[must_use]
    pub fn body(&self) -> QueryErrorBody {
        match self {
            Self::UnsupportedResultType { data_type } => QueryErrorBody {
                code: UNSUPPORTED_RESULT_TYPE,
                data_type: Some(data_type.clone()),
            },
            Self::ResponseTooLarge { .. } => QueryErrorBody {
                code: RESPONSE_TOO_LARGE,
                data_type: None,
            },
        }
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedResultType { data_type } => {
                write!(formatter, "unsupported result type: {data_type}")
            }
            Self::ResponseTooLarge { .. } => formatter.write_str("response too large"),
        }
    }
}

impl Error for EncodeError {}

/// Encode `{ "schema", "rows", "metrics" }` directly into one buffer.
///
/// The returned bytes are a complete document. A failure leaves the caller with [`EncodeError`]
/// and no partial JSON.
///
/// # Errors
///
/// Returns [`EncodeError::UnsupportedResultType`] when a column type cannot be represented, and
/// [`EncodeError::ResponseTooLarge`] when the document would exceed `max_bytes`.
#[must_use = "the encoded document is the query response"]
pub fn encode_query_response(
    schema: &Schema,
    batches: &[RecordBatch],
    metrics: QueryMetrics,
    max_bytes: usize,
) -> Result<Vec<u8>, EncodeError> {
    let fields = schema_fields(schema)?;
    let mut buf = Buf::new(max_bytes);
    buf.push_str("{\"schema\":")?;
    write_schema(&mut buf, &fields)?;
    buf.push_str(",\"rows\":")?;
    write_rows(&mut buf, schema, batches)?;
    buf.push_str(",\"metrics\":")?;
    write_metrics(&mut buf, QueryMetricsBody::from_metrics(metrics))?;
    buf.push_str("}")?;
    Ok(buf.bytes)
}

/// Encode `{ "error": { "code", ... } }` for a failed query.
#[must_use]
pub fn encode_error(error: &EncodeError) -> Vec<u8> {
    let body = error.body();
    serde_json::to_vec(&QueryErrorResponse { error: &body }).expect("query error json")
}

fn schema_fields(schema: &Schema) -> Result<Vec<SchemaField>, EncodeError> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Ok(SchemaField {
                name: field.name().clone(),
                data_type: type_name(field.data_type())?,
                nullable: field.is_nullable(),
            })
        })
        .collect()
}

fn duration_nanos(duration: Duration) -> u128 {
    duration.as_nanos()
}

struct Buf {
    bytes: Vec<u8>,
    limit: usize,
}

impl Buf {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        let next = self.bytes.len().saturating_add(bytes.len());
        if next > self.limit {
            return Err(EncodeError::ResponseTooLarge {
                max_bytes: self.limit,
            });
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn push_str(&mut self, value: &str) -> Result<(), EncodeError> {
        self.push(value.as_bytes())
    }
}

fn write_schema(buf: &mut Buf, fields: &[SchemaField]) -> Result<(), EncodeError> {
    buf.push_str("[")?;
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            buf.push_str(",")?;
        }
        buf.push_str("{\"name\":")?;
        write_json_string(buf, &field.name)?;
        buf.push_str(",\"type\":")?;
        write_json_string(buf, &field.data_type)?;
        buf.push_str(",\"nullable\":")?;
        buf.push_str(if field.nullable { "true" } else { "false" })?;
        buf.push_str("}")?;
    }
    buf.push_str("]")
}

fn write_rows(buf: &mut Buf, schema: &Schema, batches: &[RecordBatch]) -> Result<(), EncodeError> {
    buf.push_str("[")?;
    let mut started = false;
    for batch in batches {
        if batch.num_columns() != schema.fields().len() {
            return Err(unsupported_type("record batch"));
        }
        for row in 0..batch.num_rows() {
            if started {
                buf.push_str(",")?;
            }
            started = true;
            write_row(buf, schema, batch, row)?;
        }
    }
    buf.push_str("]")
}

fn write_row(
    buf: &mut Buf,
    schema: &Schema,
    batch: &RecordBatch,
    row: usize,
) -> Result<(), EncodeError> {
    buf.push_str("{")?;
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            buf.push_str(",")?;
        }
        write_json_string(buf, field.name())?;
        buf.push_str(":")?;
        write_value(buf, batch.column(index).as_ref(), row)?;
    }
    buf.push_str("}")
}

fn write_metrics(buf: &mut Buf, metrics: QueryMetricsBody) -> Result<(), EncodeError> {
    buf.push_str("{\"admission_wait_ns\":")?;
    write_quoted(buf, &metrics.admission_wait_ns.to_string())?;
    buf.push_str(",\"planning_ns\":")?;
    write_quoted(buf, &metrics.planning_ns.to_string())?;
    buf.push_str(",\"execution_ns\":")?;
    write_quoted(buf, &metrics.execution_ns.to_string())?;
    buf.push_str(",\"files_scanned\":")?;
    write_quoted(buf, &metrics.files_scanned.to_string())?;
    buf.push_str(",\"files_pruned\":")?;
    write_quoted(buf, &metrics.files_pruned.to_string())?;
    buf.push_str(",\"row_groups_pruned\":")?;
    write_quoted(buf, &metrics.row_groups_pruned.to_string())?;
    buf.push_str(",\"rows_returned\":")?;
    write_quoted(buf, &metrics.rows_returned.to_string())?;
    buf.push_str(",\"memory_peak_bytes\":")?;
    write_quoted(buf, &metrics.memory_peak_bytes.to_string())?;
    buf.push_str(",\"spill_bytes\":")?;
    write_quoted(buf, &metrics.spill_bytes.to_string())?;
    buf.push_str(",\"timed_out\":")?;
    buf.push_str(if metrics.timed_out { "true" } else { "false" })?;
    buf.push_str(",\"cancelled\":")?;
    buf.push_str(if metrics.cancelled { "true" } else { "false" })?;
    buf.push_str("}")
}

fn write_value(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    if array.is_null(index) {
        return buf.push_str("null");
    }
    match array.data_type() {
        DataType::Null => buf.push_str("null"),
        DataType::Boolean => write_bool(buf, array, index),
        DataType::Int8 => write_number::<Int8Array>(buf, array, index),
        DataType::Int16 => write_number::<Int16Array>(buf, array, index),
        DataType::Int32 => write_number::<Int32Array>(buf, array, index),
        DataType::Int64 => write_quoted_array::<Int64Array>(buf, array, index),
        DataType::UInt8 => write_number::<UInt8Array>(buf, array, index),
        DataType::UInt16 => write_number::<UInt16Array>(buf, array, index),
        DataType::UInt32 => write_number::<UInt32Array>(buf, array, index),
        DataType::UInt64 => write_quoted_array::<UInt64Array>(buf, array, index),
        DataType::Float16 => write_float16(buf, array, index),
        DataType::Float32 => write_float32(buf, array, index),
        DataType::Float64 => write_float64(buf, array, index),
        DataType::Utf8 => write_string::<StringArray>(buf, array, index),
        DataType::LargeUtf8 => write_string::<LargeStringArray>(buf, array, index),
        DataType::Utf8View => write_string::<StringViewArray>(buf, array, index),
        DataType::Binary => write_binary::<BinaryArray>(buf, array, index),
        DataType::LargeBinary => write_binary::<LargeBinaryArray>(buf, array, index),
        DataType::BinaryView => write_binary::<BinaryViewArray>(buf, array, index),
        DataType::FixedSizeBinary(_) => write_fixed_binary(buf, array, index),
        DataType::Timestamp(TimeUnit::Second, _) => {
            write_quoted_array::<TimestampSecondArray>(buf, array, index)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            write_quoted_array::<TimestampMillisecondArray>(buf, array, index)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            write_quoted_array::<TimestampMicrosecondArray>(buf, array, index)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            write_quoted_array::<TimestampNanosecondArray>(buf, array, index)
        }
        DataType::Date32 => write_quoted_array::<Date32Array>(buf, array, index),
        DataType::Date64 => write_quoted_array::<Date64Array>(buf, array, index),
        DataType::Time32(TimeUnit::Second) => {
            write_quoted_array::<Time32SecondArray>(buf, array, index)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            write_quoted_array::<Time32MillisecondArray>(buf, array, index)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            write_quoted_array::<Time64MicrosecondArray>(buf, array, index)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            write_quoted_array::<Time64NanosecondArray>(buf, array, index)
        }
        DataType::Duration(TimeUnit::Second) => {
            write_quoted_array::<DurationSecondArray>(buf, array, index)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            write_quoted_array::<DurationMillisecondArray>(buf, array, index)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            write_quoted_array::<DurationMicrosecondArray>(buf, array, index)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            write_quoted_array::<DurationNanosecondArray>(buf, array, index)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            write_quoted_array::<IntervalYearMonthArray>(buf, array, index)
        }
        DataType::Interval(IntervalUnit::DayTime) => write_day_time(buf, array, index),
        DataType::Interval(IntervalUnit::MonthDayNano) => write_month_day_nano(buf, array, index),
        DataType::Decimal32(_, _) => write_decimal::<Decimal32Array>(buf, array, index),
        DataType::Decimal64(_, _) => write_decimal::<Decimal64Array>(buf, array, index),
        DataType::Decimal128(_, _) => write_decimal::<Decimal128Array>(buf, array, index),
        DataType::Decimal256(_, _) => write_decimal::<Decimal256Array>(buf, array, index),
        DataType::List(_) => write_list::<ListArray>(buf, array, index),
        DataType::LargeList(_) => write_list::<LargeListArray>(buf, array, index),
        DataType::FixedSizeList(_, _) => write_fixed_list(buf, array, index),
        DataType::Struct(_) => write_struct(buf, array, index),
        DataType::Map(_, _) => write_map(buf, array, index),
        DataType::Dictionary(key, _) => match key.as_ref() {
            DataType::Int8 => write_dictionary::<Int8Type>(buf, array, index),
            DataType::Int16 => write_dictionary::<Int16Type>(buf, array, index),
            DataType::Int32 => write_dictionary::<Int32Type>(buf, array, index),
            DataType::Int64 => write_dictionary::<Int64Type>(buf, array, index),
            DataType::UInt8 => write_dictionary::<UInt8Type>(buf, array, index),
            DataType::UInt16 => write_dictionary::<UInt16Type>(buf, array, index),
            DataType::UInt32 => write_dictionary::<UInt32Type>(buf, array, index),
            DataType::UInt64 => write_dictionary::<UInt64Type>(buf, array, index),
            _ => Err(unsupported_type("Dictionary")),
        },
        DataType::Time32(_) => Err(unsupported_type("Time32")),
        DataType::Time64(_) => Err(unsupported_type("Time64")),
        DataType::ListView(_) => Err(unsupported_type("ListView")),
        DataType::LargeListView(_) => Err(unsupported_type("LargeListView")),
        DataType::Union(_, _) => Err(unsupported_type("Union")),
        DataType::RunEndEncoded(_, _) => Err(unsupported_type("RunEndEncoded")),
    }
}

fn write_bool(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<BooleanArray>(array)?;
    buf.push_str(if values.value(index) { "true" } else { "false" })
}

trait ArrayValue {
    type Value;
    fn read(&self, index: usize) -> Self::Value;
}

macro_rules! primitive_value {
    ($array:ty, $value:ty) => {
        impl ArrayValue for $array {
            type Value = $value;
            fn read(&self, index: usize) -> Self::Value {
                self.value(index)
            }
        }
    };
}

primitive_value!(Int8Array, i8);
primitive_value!(Int16Array, i16);
primitive_value!(Int32Array, i32);
primitive_value!(Int64Array, i64);
primitive_value!(UInt8Array, u8);
primitive_value!(UInt16Array, u16);
primitive_value!(UInt32Array, u32);
primitive_value!(UInt64Array, u64);
primitive_value!(TimestampSecondArray, i64);
primitive_value!(TimestampMillisecondArray, i64);
primitive_value!(TimestampMicrosecondArray, i64);
primitive_value!(TimestampNanosecondArray, i64);
primitive_value!(Date32Array, i32);
primitive_value!(Date64Array, i64);
primitive_value!(Time32SecondArray, i32);
primitive_value!(Time32MillisecondArray, i32);
primitive_value!(Time64MicrosecondArray, i64);
primitive_value!(Time64NanosecondArray, i64);
primitive_value!(DurationSecondArray, i64);
primitive_value!(DurationMillisecondArray, i64);
primitive_value!(DurationMicrosecondArray, i64);
primitive_value!(DurationNanosecondArray, i64);
primitive_value!(IntervalYearMonthArray, i32);

fn write_number<T>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    T: Array + ArrayValue + 'static,
    T::Value: fmt::Display,
{
    let values = downcast::<T>(array)?;
    buf.push_str(&values.read(index).to_string())
}

fn write_quoted_array<T>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    T: Array + ArrayValue + 'static,
    T::Value: fmt::Display,
{
    let values = downcast::<T>(array)?;
    write_quoted(buf, &values.read(index).to_string())
}

fn write_float16(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<Float16Array>(array)?;
    write_finite(buf, f64::from(values.value(index).to_f32()))
}

fn write_float32(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<Float32Array>(array)?;
    write_finite(buf, f64::from(values.value(index)))
}

fn write_float64(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<Float64Array>(array)?;
    write_finite(buf, values.value(index))
}

fn write_finite(buf: &mut Buf, value: f64) -> Result<(), EncodeError> {
    if value.is_finite() {
        buf.push_str(&value.to_string())
    } else {
        buf.push_str("null")
    }
}

trait TextValue {
    fn text(&self, index: usize) -> &str;
}

impl TextValue for StringArray {
    fn text(&self, index: usize) -> &str {
        self.value(index)
    }
}

impl TextValue for LargeStringArray {
    fn text(&self, index: usize) -> &str {
        self.value(index)
    }
}

impl TextValue for StringViewArray {
    fn text(&self, index: usize) -> &str {
        self.value(index)
    }
}

fn write_string<T>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    T: Array + TextValue + 'static,
{
    let values = downcast::<T>(array)?;
    write_json_string(buf, values.text(index))
}

trait BytesValue {
    fn bytes(&self, index: usize) -> &[u8];
}

impl BytesValue for BinaryArray {
    fn bytes(&self, index: usize) -> &[u8] {
        self.value(index)
    }
}

impl BytesValue for LargeBinaryArray {
    fn bytes(&self, index: usize) -> &[u8] {
        self.value(index)
    }
}

impl BytesValue for BinaryViewArray {
    fn bytes(&self, index: usize) -> &[u8] {
        self.value(index)
    }
}

impl BytesValue for FixedSizeBinaryArray {
    fn bytes(&self, index: usize) -> &[u8] {
        self.value(index)
    }
}

fn write_binary<T>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    T: Array + BytesValue + 'static,
{
    let values = downcast::<T>(array)?;
    write_base64(buf, values.bytes(index))
}

fn write_fixed_binary(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<FixedSizeBinaryArray>(array)?;
    write_base64(buf, values.value(index))
}

fn write_base64(buf: &mut Buf, bytes: &[u8]) -> Result<(), EncodeError> {
    write_json_string(buf, &STANDARD.encode(bytes))
}

trait DecimalValue {
    fn decimal(&self, index: usize) -> String;
}

impl DecimalValue for Decimal32Array {
    fn decimal(&self, index: usize) -> String {
        self.value_as_string(index)
    }
}

impl DecimalValue for Decimal64Array {
    fn decimal(&self, index: usize) -> String {
        self.value_as_string(index)
    }
}

impl DecimalValue for Decimal128Array {
    fn decimal(&self, index: usize) -> String {
        self.value_as_string(index)
    }
}

impl DecimalValue for Decimal256Array {
    fn decimal(&self, index: usize) -> String {
        self.value_as_string(index)
    }
}

fn write_decimal<T>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    T: Array + DecimalValue + 'static,
{
    let values = downcast::<T>(array)?;
    write_json_string(buf, &values.decimal(index))
}

fn write_day_time(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<IntervalDayTimeArray>(array)?;
    let value = values.value(index);
    write_json_string(buf, &format!("{},{}", value.days, value.milliseconds))
}

fn write_month_day_nano(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<IntervalMonthDayNanoArray>(array)?;
    let value = values.value(index);
    write_json_string(
        buf,
        &format!("{},{},{}", value.months, value.days, value.nanoseconds),
    )
}

trait ListValue {
    fn child(&self, index: usize) -> ArrayRef;
}

impl ListValue for ListArray {
    fn child(&self, index: usize) -> ArrayRef {
        self.value(index)
    }
}

impl ListValue for LargeListArray {
    fn child(&self, index: usize) -> ArrayRef {
        self.value(index)
    }
}

fn write_list<T>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    T: Array + ListValue + 'static,
{
    let values = downcast::<T>(array)?;
    write_sequence(buf, values.child(index).as_ref())
}

fn write_fixed_list(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<FixedSizeListArray>(array)?;
    write_sequence(buf, values.value(index).as_ref())
}

fn write_sequence(buf: &mut Buf, values: &dyn Array) -> Result<(), EncodeError> {
    buf.push_str("[")?;
    for index in 0..values.len() {
        if index > 0 {
            buf.push_str(",")?;
        }
        write_value(buf, values, index)?;
    }
    buf.push_str("]")
}

fn write_struct(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<StructArray>(array)?;
    buf.push_str("{")?;
    for (position, field) in values.fields().iter().enumerate() {
        if position > 0 {
            buf.push_str(",")?;
        }
        write_json_string(buf, field.name())?;
        buf.push_str(":")?;
        write_value(buf, values.column(position).as_ref(), index)?;
    }
    buf.push_str("}")
}

fn write_map(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError> {
    let values = downcast::<MapArray>(array)?;
    let entries = values.value(index);
    buf.push_str("[")?;
    for row in 0..entries.len() {
        if row > 0 {
            buf.push_str(",")?;
        }
        buf.push_str("{\"key\":")?;
        write_value(buf, entries.column(0).as_ref(), row)?;
        buf.push_str(",\"value\":")?;
        write_value(buf, entries.column(1).as_ref(), row)?;
        buf.push_str("}")?;
    }
    buf.push_str("]")
}

fn write_dictionary<K>(buf: &mut Buf, array: &dyn Array, index: usize) -> Result<(), EncodeError>
where
    K: ArrowDictionaryKeyType,
{
    let values = downcast::<DictionaryArray<K>>(array)?;
    match values.key(index) {
        None => buf.push_str("null"),
        Some(key) if key < values.values().len() => write_value(buf, values.values().as_ref(), key),
        Some(_) => Err(unsupported_type("Dictionary")),
    }
}

fn write_quoted(buf: &mut Buf, value: &str) -> Result<(), EncodeError> {
    buf.push_str("\"")?;
    buf.push_str(value)?;
    buf.push_str("\"")
}

fn write_json_string(buf: &mut Buf, value: &str) -> Result<(), EncodeError> {
    buf.push_str("\"")?;
    let mut start = 0;
    for (index, character) in value.char_indices() {
        let escaped = match character {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            control if u32::from(control) < 0x20 => {
                buf.push_str(&value[start..index])?;
                let encoded = format!("\\u{:04x}", u32::from(control));
                buf.push_str(&encoded)?;
                start = index + character.len_utf8();
                continue;
            }
            _ => continue,
        };
        buf.push_str(&value[start..index])?;
        buf.push_str(escaped)?;
        start = index + character.len_utf8();
    }
    buf.push_str(&value[start..])?;
    buf.push_str("\"")
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T, EncodeError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| type_error(array.data_type()))
}

fn type_error(data_type: &DataType) -> EncodeError {
    match type_name(data_type) {
        Ok(data_type) => EncodeError::UnsupportedResultType { data_type },
        Err(error) => error,
    }
}

fn unsupported_type(data_type: &str) -> EncodeError {
    EncodeError::UnsupportedResultType {
        data_type: data_type.to_owned(),
    }
}

fn type_name(data_type: &DataType) -> Result<String, EncodeError> {
    let name = match data_type {
        DataType::Null => "Null",
        DataType::Boolean => "Boolean",
        DataType::Int8 => "Int8",
        DataType::Int16 => "Int16",
        DataType::Int32 => "Int32",
        DataType::Int64 => "Int64",
        DataType::UInt8 => "UInt8",
        DataType::UInt16 => "UInt16",
        DataType::UInt32 => "UInt32",
        DataType::UInt64 => "UInt64",
        DataType::Float16 => "Float16",
        DataType::Float32 => "Float32",
        DataType::Float64 => "Float64",
        DataType::Utf8 => "Utf8",
        DataType::LargeUtf8 => "LargeUtf8",
        DataType::Utf8View => "Utf8View",
        DataType::Binary => "Binary",
        DataType::LargeBinary => "LargeBinary",
        DataType::BinaryView => "BinaryView",
        DataType::Date32 => "Date32",
        DataType::Date64 => "Date64",
        other => return compound_type(other),
    };
    Ok(name.to_owned())
}

fn compound_type(data_type: &DataType) -> Result<String, EncodeError> {
    match data_type {
        DataType::FixedSizeBinary(size) => Ok(format!("FixedSizeBinary({size})")),
        DataType::Timestamp(unit, zone) => Ok(format!(
            "Timestamp({}, {})",
            time_unit(*unit),
            zone_name(zone)
        )),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second | TimeUnit::Millisecond => Ok(format!("Time32({})", time_unit(*unit))),
            _ => Err(unsupported_type("Time32")),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond | TimeUnit::Nanosecond => {
                Ok(format!("Time64({})", time_unit(*unit)))
            }
            _ => Err(unsupported_type("Time64")),
        },
        DataType::Duration(unit) => Ok(format!("Duration({})", time_unit(*unit))),
        DataType::Interval(unit) => Ok(format!("Interval({})", interval_unit(*unit))),
        DataType::Decimal32(precision, scale) => Ok(format!("Decimal32({precision}, {scale})")),
        DataType::Decimal64(precision, scale) => Ok(format!("Decimal64({precision}, {scale})")),
        DataType::Decimal128(precision, scale) => Ok(format!("Decimal128({precision}, {scale})")),
        DataType::Decimal256(precision, scale) => Ok(format!("Decimal256({precision}, {scale})")),
        DataType::List(field) => Ok(format!("List<{}>", field_desc(field)?)),
        DataType::LargeList(field) => Ok(format!("LargeList<{}>", field_desc(field)?)),
        DataType::FixedSizeList(field, size) => {
            Ok(format!("FixedSizeList<{}>({size})", field_desc(field)?))
        }
        DataType::Struct(fields) => struct_type(fields),
        DataType::Map(entry, _) => map_type(entry),
        DataType::Dictionary(key, value) => Ok(format!(
            "Dictionary<{}, {}>",
            type_name(key)?,
            type_name(value)?
        )),
        DataType::ListView(_) => Err(unsupported_type("ListView")),
        DataType::LargeListView(_) => Err(unsupported_type("LargeListView")),
        DataType::Union(_, _) => Err(unsupported_type("Union")),
        DataType::RunEndEncoded(_, _) => Err(unsupported_type("RunEndEncoded")),
        _ => Err(unsupported_type("unknown")),
    }
}

fn field_desc(field: &Field) -> Result<String, EncodeError> {
    let inner = type_name(field.data_type())?;
    if field.is_nullable() {
        Ok(format!("nullable {inner}"))
    } else {
        Ok(inner)
    }
}

fn struct_type(fields: &Fields) -> Result<String, EncodeError> {
    let mut parts = Vec::with_capacity(fields.len());
    for field in fields {
        parts.push(format!("{}: {}", field.name(), field_desc(field)?));
    }
    Ok(format!("Struct<{}>", parts.join(", ")))
}

fn map_type(entry: &Field) -> Result<String, EncodeError> {
    let DataType::Struct(children) = entry.data_type() else {
        return Err(unsupported_type("Map"));
    };
    if children.len() != 2 {
        return Err(unsupported_type("Map"));
    }
    Ok(format!(
        "Map<{}, {}>",
        field_desc(&children[0])?,
        field_desc(&children[1])?
    ))
}

fn time_unit(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "Second",
        TimeUnit::Millisecond => "Millisecond",
        TimeUnit::Microsecond => "Microsecond",
        TimeUnit::Nanosecond => "Nanosecond",
    }
}

fn interval_unit(unit: IntervalUnit) -> &'static str {
    match unit {
        IntervalUnit::YearMonth => "YearMonth",
        IntervalUnit::DayTime => "DayTime",
        IntervalUnit::MonthDayNano => "MonthDayNano",
    }
}

fn zone_name(zone: &Option<Arc<str>>) -> String {
    match zone {
        None => "None".to_owned(),
        Some(name) => format!("\"{name}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EncodeError, QueryRequest, RESPONSE_TOO_LARGE, UNSUPPORTED_RESULT_TYPE, encode_error,
        encode_query_response,
    };
    use crate::config::DEFAULT_QUERY_MAX_RESPONSE_BYTES;
    use arrow_array::builder::{
        FixedSizeBinaryBuilder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
    };
    use arrow_array::types::Int32Type;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, Float64Array,
        Int32Array, Int64Array, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};
    use observer_query::QueryMetrics;
    use std::{sync::Arc, time::Duration};

    const ZERO_METRICS: &str = r#"{"admission_wait_ns":"0","planning_ns":"0","execution_ns":"0","files_scanned":"0","files_pruned":"0","row_groups_pruned":"0","rows_returned":"0","memory_peak_bytes":"0","spill_bytes":"0","timed_out":false,"cancelled":false}"#;

    fn document(schema: &Schema, batches: &[RecordBatch], metrics: QueryMetrics) -> String {
        let bytes = encode_query_response(schema, batches, metrics, usize::MAX).expect("encode");
        String::from_utf8(bytes).expect("utf8")
    }

    fn one_batch(schema: &Schema, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(schema.clone()), columns).expect("batch")
    }

    #[test]
    fn request_rejects_unknown_fields_and_keeps_optional_caps() {
        let request: QueryRequest = serde_json::from_str(r#"{"sql":"select 1"}"#).expect("parse");
        assert_eq!(request.sql, "select 1");
        assert_eq!(request.timeout_ms, None);
        assert_eq!(request.max_rows, None);
        let request: QueryRequest =
            serde_json::from_str(r#"{"sql":"select 1","timeout_ms":10,"max_rows":2}"#)
                .expect("parse");
        assert_eq!(request.timeout_ms, Some(10));
        assert_eq!(request.max_rows, Some(2));
        assert!(
            serde_json::from_str::<QueryRequest>(r#"{"sql":"select 1","tenant":"a"}"#).is_err()
        );
    }

    #[test]
    fn schema_keeps_order_aliases_and_nullability() {
        let schema = Schema::new(vec![
            Field::new("rows_counted", DataType::Int32, false),
            Field::new("message", DataType::Utf8, true),
            Field::new("ok", DataType::Boolean, false),
        ]);
        let first = one_batch(
            &schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("alias")])),
                Arc::new(BooleanArray::from(vec![false])),
            ],
        );
        let second = one_batch(
            &schema,
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        );
        let text = document(&schema, &[first, second], QueryMetrics::default());
        assert_eq!(
            text,
            format!(
                r#"{{"schema":[{{"name":"rows_counted","type":"Int32","nullable":false}},{{"name":"message","type":"Utf8","nullable":true}},{{"name":"ok","type":"Boolean","nullable":false}}],"rows":[{{"rows_counted":1,"message":"alias","ok":false}},{{"rows_counted":2,"message":null,"ok":true}}],"metrics":{ZERO_METRICS}}}"#
            )
        );
    }

    #[test]
    fn nulls_and_non_finite_floats_are_json_null() {
        let schema = Schema::new(vec![
            Field::new("code", DataType::Int32, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("note", DataType::Utf8, true),
        ]);
        let batch = one_batch(
            &schema,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(1), Some(2)])),
                Arc::new(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(0.5),
                ])),
                Arc::new(StringArray::from(vec![None, Some("a\"b\\c\n"), Some("ok")])),
            ],
        );
        let text = document(&schema, &[batch], QueryMetrics::default());
        assert!(text.contains(
            r#""rows":[{"code":null,"ratio":null,"note":null},{"code":1,"ratio":null,"note":"a\"b\\c\n"},{"code":2,"ratio":0.5,"note":"ok"}]"#
        ));
    }

    #[test]
    fn integers_and_nanosecond_timestamps_stay_decimal_strings() {
        let event = UInt64Array::from(vec![1_700_000_000_000_000_000_u64]);
        let delta = Int64Array::from(vec![i64::MIN]);
        let small = Int64Array::from(vec![42_i64]);
        let width = Int32Array::from(vec![42]);
        let seen = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_001_i64]);
        let top = UInt64Array::from(vec![u64::MAX]);
        let schema = Schema::new(vec![
            Field::new("event_time_unix_nano", event.data_type().clone(), false),
            Field::new("delta", delta.data_type().clone(), false),
            Field::new("small", small.data_type().clone(), false),
            Field::new("width", width.data_type().clone(), false),
            Field::new("seen", seen.data_type().clone(), false),
            Field::new("top", top.data_type().clone(), false),
        ]);
        let batch = one_batch(
            &schema,
            vec![
                Arc::new(event),
                Arc::new(delta),
                Arc::new(small),
                Arc::new(width),
                Arc::new(seen),
                Arc::new(top),
            ],
        );
        let text = document(&schema, &[batch], QueryMetrics::default());
        assert!(text.contains(r#""type":"Timestamp(Nanosecond, None)""#));
        assert!(text.contains(
            r#""rows":[{"event_time_unix_nano":"1700000000000000000","delta":"-9223372036854775808","small":"42","width":42,"seen":"1700000000000000001","top":"18446744073709551615"}]"#
        ));
    }

    #[test]
    fn binary_values_are_standard_base64() {
        let payload = BinaryArray::from(vec![Some(&b"\x00\x01\x02"[..])]);
        let mut trace_builder = FixedSizeBinaryBuilder::new(2);
        trace_builder.append_value([0_u8, 1]).expect("trace");
        let trace = trace_builder.finish();
        let schema = Schema::new(vec![
            Field::new("payload", payload.data_type().clone(), true),
            Field::new("trace_id", trace.data_type().clone(), true),
        ]);
        let batch = one_batch(&schema, vec![Arc::new(payload), Arc::new(trace)]);
        let text = document(&schema, &[batch], QueryMetrics::default());
        assert!(text.contains(r#""type":"Binary""#));
        assert!(text.contains(r#""type":"FixedSizeBinary(2)""#));
        assert!(text.contains(r#""rows":[{"payload":"AAEC","trace_id":"AAE="}]"#));
    }

    #[test]
    fn nested_list_struct_and_map_values_stay_nested() {
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("a");
        tags.values().append_null();
        tags.append(true);
        let tags = tags.finish();

        let mut attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new())
            .with_values_field(Field::new("values", DataType::Int64, false));
        attrs.keys().append_value("n");
        attrs.values().append_value(1);
        attrs.append(true).expect("map");
        let attrs = attrs.finish();

        let meta = StructArray::try_new(
            Fields::from(vec![Field::new("attrs", attrs.data_type().clone(), false)]),
            vec![Arc::new(attrs)],
            None,
        )
        .expect("struct");
        let id = Int32Array::from(vec![7]);
        let schema = Schema::new(vec![
            Field::new("id", id.data_type().clone(), false),
            Field::new("tags", tags.data_type().clone(), false),
            Field::new("meta", meta.data_type().clone(), false),
        ]);
        let batch = one_batch(&schema, vec![Arc::new(id), Arc::new(tags), Arc::new(meta)]);
        let text = document(&schema, &[batch], QueryMetrics::default());
        assert!(text.contains(r#""type":"List<nullable Utf8>""#));
        assert!(text.contains(r#""type":"Struct<attrs: Map<Utf8, Int64>>""#));
        assert!(text.contains(
            r#""rows":[{"id":7,"tags":["a",null],"meta":{"attrs":[{"key":"n","value":"1"}]}}]"#
        ));
    }

    #[test]
    fn aggregate_columns_keep_counts_averages_and_decimals() {
        let service = DictionaryArray::<Int32Type>::from_iter([Some("api")]);
        let count = Int64Array::from(vec![3_i64]);
        let average = Float64Array::from(vec![1.5_f64]);
        let total = Decimal128Array::from(vec![12_345_i128])
            .with_precision_and_scale(10, 2)
            .expect("decimal");
        let schema = Schema::new(vec![
            Field::new("service", service.data_type().clone(), false),
            Field::new("count", count.data_type().clone(), false),
            Field::new("average", average.data_type().clone(), true),
            Field::new("total", total.data_type().clone(), true),
        ]);
        let batch = one_batch(
            &schema,
            vec![
                Arc::new(service),
                Arc::new(count),
                Arc::new(average),
                Arc::new(total),
            ],
        );
        let text = document(&schema, &[batch], QueryMetrics::default());
        assert!(text.contains(r#""type":"Dictionary<Int32, Utf8>""#));
        assert!(text.contains(r#""type":"Decimal128(10, 2)""#));
        assert!(
            text.contains(
                r#""rows":[{"service":"api","count":"3","average":1.5,"total":"123.45"}]"#
            )
        );
    }

    #[test]
    fn empty_results_still_carry_schema_and_metrics() {
        let schema = Schema::new(vec![Field::new("body", DataType::Utf8, true)]);
        let metrics = QueryMetrics {
            admission_wait: Duration::from_nanos(1_500_000_000),
            planning: Duration::from_nanos(20),
            execution: Duration::from_nanos(3),
            files_scanned: 2,
            files_pruned: 4,
            row_groups_pruned: 8,
            rows_returned: 0,
            memory_peak_bytes: 9_007_199_254_740_993,
            spill_bytes: 1,
            timed_out: false,
            cancelled: true,
        };
        let text = document(&schema, &[], metrics);
        assert_eq!(
            text,
            r#"{"schema":[{"name":"body","type":"Utf8","nullable":true}],"rows":[],"metrics":{"admission_wait_ns":"1500000000","planning_ns":"20","execution_ns":"3","files_scanned":"2","files_pruned":"4","row_groups_pruned":"8","rows_returned":"0","memory_peak_bytes":"9007199254740993","spill_bytes":"1","timed_out":false,"cancelled":true}}"#
        );
    }

    #[test]
    fn response_fits_exactly_16_mib_and_rejects_one_more_byte() {
        let schema = Schema::new(vec![Field::new("body", DataType::Utf8, false)]);
        let empty = one_batch(&schema, vec![Arc::new(StringArray::from(vec![""]))]);
        let empty_bytes =
            encode_query_response(&schema, &[empty], QueryMetrics::default(), usize::MAX)
                .expect("empty");
        let fill = DEFAULT_QUERY_MAX_RESPONSE_BYTES - empty_bytes.len();
        let body = "a".repeat(fill);
        let exact = one_batch(
            &schema,
            vec![Arc::new(StringArray::from(vec![body.as_str()]))],
        );
        let encoded = encode_query_response(
            &schema,
            std::slice::from_ref(&exact),
            QueryMetrics::default(),
            DEFAULT_QUERY_MAX_RESPONSE_BYTES,
        )
        .expect("exact");
        assert_eq!(encoded.len(), DEFAULT_QUERY_MAX_RESPONSE_BYTES);
        let error = encode_query_response(
            &schema,
            std::slice::from_ref(&exact),
            QueryMetrics::default(),
            DEFAULT_QUERY_MAX_RESPONSE_BYTES - 1,
        )
        .expect_err("one byte over");
        assert_eq!(
            error,
            EncodeError::ResponseTooLarge {
                max_bytes: DEFAULT_QUERY_MAX_RESPONSE_BYTES - 1
            }
        );
        assert_eq!(
            encode_error(&error),
            format!(r#"{{"error":{{"code":"{RESPONSE_TOO_LARGE}"}}}}"#).into_bytes()
        );
    }

    #[test]
    fn unsupported_result_types_fail_before_a_partial_document() {
        let schema = Schema::new(vec![Field::new(
            "values",
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let error = encode_query_response(&schema, &[], QueryMetrics::default(), usize::MAX)
            .expect_err("type");
        assert_eq!(
            error,
            EncodeError::UnsupportedResultType {
                data_type: "ListView".to_owned()
            }
        );
        assert_eq!(
            encode_error(&error),
            format!(r#"{{"error":{{"code":"{UNSUPPORTED_RESULT_TYPE}","data_type":"ListView"}}}}"#)
                .into_bytes()
        );
    }
}
