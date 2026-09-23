//! Conservative min/max and null counts for one published Parquet file.
//!
//! Bounds cover scalar core columns and typed dynamic columns. Binary values, canonical JSON,
//! dynamic JSON, and any column that contains a non-finite float have a null count and no bounds.
//! A file whose statistics are missing or fail [`statistics_usable`] must be scanned.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use crate::dynamic::{DynamicKind, FIELD_KIND};
use crate::{
    COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_LOG_ATTRIBUTES, COLUMN_RESOURCE_ATTRIBUTES,
    COLUMN_SCOPE_ATTRIBUTES, COLUMN_WAL_SEQUENCE,
};

/// Inclusive bounds for one non-null scalar value.
#[derive(Clone, Debug)]
pub enum StatValue {
    /// Boolean, with `false` ordered before `true`.
    Bool(bool),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 16-bit integer.
    UInt16(u16),
    /// Unsigned 32-bit integer.
    UInt32(u32),
    /// Unsigned 64-bit integer.
    UInt64(u64),
    /// Finite 64-bit float.
    Float64(f64),
    /// UTF-8 text, ordered lexicographically.
    Utf8(String),
}

impl PartialEq for StatValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Int32(left), Self::Int32(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            (Self::UInt16(left), Self::UInt16(right)) => left == right,
            (Self::UInt32(left), Self::UInt32(right)) => left == right,
            (Self::UInt64(left), Self::UInt64(right)) => left == right,
            (Self::Float64(left), Self::Float64(right)) => left == right,
            (Self::Utf8(left), Self::Utf8(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for StatValue {}

impl StatValue {
    /// `Some(true)` when both values have the same type and `self` is strictly smaller.
    #[must_use]
    pub fn less_than(&self, other: &Self) -> Option<bool> {
        Some(match (self, other) {
            (Self::Bool(left), Self::Bool(right)) => left < right,
            (Self::Int32(left), Self::Int32(right)) => left < right,
            (Self::Int64(left), Self::Int64(right)) => left < right,
            (Self::UInt16(left), Self::UInt16(right)) => left < right,
            (Self::UInt32(left), Self::UInt32(right)) => left < right,
            (Self::UInt64(left), Self::UInt64(right)) => left < right,
            (Self::Float64(left), Self::Float64(right)) => left < right,
            (Self::Utf8(left), Self::Utf8(right)) => left < right,
            _ => return None,
        })
    }

    /// Arrow type stored for this value.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Bool(_) => DataType::Boolean,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
        }
    }
}

/// Null count and optional inclusive bounds for one column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnStatistics {
    /// Physical column name.
    pub name: String,
    /// Nulls in this column, including rows whose generation lacks the column.
    pub null_count: u64,
    /// Inclusive minimum and maximum of the non-null values.
    pub bounds: Option<(StatValue, StatValue)>,
}

/// Statistics recorded for one hour file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileStatistics {
    /// Byte length of the Parquet file.
    pub size_bytes: u64,
    /// Rows stored in the file.
    pub rows: u64,
    /// Inclusive minimum `event_time_unix_nano`.
    pub event_time_min: u64,
    /// Inclusive maximum `event_time_unix_nano`.
    pub event_time_max: u64,
    /// Inclusive minimum `wal_sequence`.
    pub wal_min: u64,
    /// Inclusive maximum `wal_sequence`.
    pub wal_max: u64,
    /// One entry per schema field, in schema order.
    pub columns: Vec<ColumnStatistics>,
}

/// Summarize `batches`, which already share `schema`.
///
/// Returns [`None`] when event time or WAL sequence has no finite bounds.
#[must_use]
pub fn file_statistics(
    schema: &Schema,
    batches: &[RecordBatch],
    size_bytes: u64,
) -> Option<FileStatistics> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(column_statistics(field.as_ref(), batches)?);
    }
    let event_time = u64_bounds(&columns, COLUMN_EVENT_TIME_UNIX_NANO)?;
    let wal = u64_bounds(&columns, COLUMN_WAL_SEQUENCE)?;
    let rows = batches.iter().try_fold(0_u64, |total, batch| {
        total.checked_add(u64::try_from(batch.num_rows()).ok()?)
    })?;
    Some(FileStatistics {
        size_bytes,
        rows,
        event_time_min: event_time.0,
        event_time_max: event_time.1,
        wal_min: wal.0,
        wal_max: wal.1,
        columns,
    })
}

/// `statistics` can exclude a file only when every bound agrees with `schema` and the commit range.
#[must_use]
pub fn statistics_usable(
    statistics: &FileStatistics,
    schema: &Schema,
    rows: u64,
    hour_start: u64,
    hour_end: u64,
    first_sequence: u64,
    next_sequence: u64,
) -> bool {
    if statistics.size_bytes == 0
        || statistics.rows != rows
        || statistics.event_time_min > statistics.event_time_max
        || statistics.wal_min > statistics.wal_max
        || statistics.event_time_min < hour_start
        || statistics.event_time_max >= hour_end
        || statistics.wal_min < first_sequence
        || statistics.wal_max >= next_sequence
        || statistics.columns.len() != schema.fields().len()
    {
        return false;
    }
    for (column, field) in statistics.columns.iter().zip(schema.fields()) {
        if column.name.as_str() != field.name().as_str() || column.null_count > rows {
            return false;
        }
        if !field.is_nullable() && column.null_count != 0 {
            return false;
        }
        match &column.bounds {
            None => {}
            Some((min, max)) => {
                if min.data_type() != *field.data_type()
                    || max.data_type() != *field.data_type()
                    || (min.less_than(max) == Some(false) && min != max)
                    || matches!(min, StatValue::Float64(value) if !value.is_finite())
                    || matches!(max, StatValue::Float64(value) if !value.is_finite())
                {
                    return false;
                }
                if min.less_than(max).is_none() {
                    return false;
                }
            }
        }
    }
    bounds_match(
        &statistics.columns,
        COLUMN_EVENT_TIME_UNIX_NANO,
        statistics.event_time_min,
        statistics.event_time_max,
    ) && bounds_match(
        &statistics.columns,
        COLUMN_WAL_SEQUENCE,
        statistics.wal_min,
        statistics.wal_max,
    )
}

fn bounds_match(columns: &[ColumnStatistics], name: &str, min: u64, max: u64) -> bool {
    columns.iter().any(|column| {
        column.name == name
            && column.bounds.as_ref().is_some_and(|(low, high)| {
                *low == StatValue::UInt64(min) && *high == StatValue::UInt64(max)
            })
    })
}

fn u64_bounds(columns: &[ColumnStatistics], name: &str) -> Option<(u64, u64)> {
    let column = columns.iter().find(|column| column.name == name)?;
    match column.bounds.as_ref()? {
        (StatValue::UInt64(min), StatValue::UInt64(max)) => Some((*min, *max)),
        _ => None,
    }
}

fn column_statistics(field: &Field, batches: &[RecordBatch]) -> Option<ColumnStatistics> {
    let mut null_count = 0_u64;
    let mut bounds = None;
    let mut drop_bounds = !bounds_supported(field);
    for batch in batches {
        let array = batch.column_by_name(field.name())?;
        if array.data_type() != field.data_type() {
            return None;
        }
        null_count = null_count.checked_add(u64::try_from(array.null_count()).ok()?)?;
        if drop_bounds {
            continue;
        }
        match fold_bounds(array, field.data_type(), &mut bounds) {
            Fold::Done => {}
            Fold::NoBounds => drop_bounds = true,
            Fold::Invalid => return None,
        }
    }
    if drop_bounds {
        bounds = None;
    }
    Some(ColumnStatistics {
        name: field.name().clone(),
        null_count,
        bounds,
    })
}

enum Fold {
    Done,
    NoBounds,
    Invalid,
}

fn fold_bounds(
    array: &dyn Array,
    data_type: &DataType,
    bounds: &mut Option<(StatValue, StatValue)>,
) -> Fold {
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let Some(value) = stat_value(array, data_type, row) else {
            return Fold::Invalid;
        };
        if matches!(value, StatValue::Float64(number) if !number.is_finite()) {
            return Fold::NoBounds;
        }
        match bounds {
            None => *bounds = Some((value.clone(), value)),
            Some((min, max)) => {
                if value.less_than(min) == Some(true) {
                    *min = value.clone();
                }
                if max.less_than(&value) == Some(true) {
                    *max = value;
                }
            }
        }
    }
    Fold::Done
}

fn stat_value(array: &dyn Array, data_type: &DataType, row: usize) -> Option<StatValue> {
    Some(match data_type {
        DataType::Boolean => StatValue::Bool(array.as_boolean().value(row)),
        DataType::Int32 => StatValue::Int32(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => StatValue::Int64(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt16 => StatValue::UInt16(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => StatValue::UInt32(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => StatValue::UInt64(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float64 => StatValue::Float64(array.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 => StatValue::Utf8(array.as_string::<i32>().value(row).to_owned()),
        _ => return None,
    })
}

fn bounds_supported(field: &Field) -> bool {
    if matches!(
        field.name().as_str(),
        COLUMN_RESOURCE_ATTRIBUTES | COLUMN_SCOPE_ATTRIBUTES | COLUMN_LOG_ATTRIBUTES
    ) || field.metadata().get(FIELD_KIND).map(String::as_str) == Some(DynamicKind::Json.as_str())
    {
        return false;
    }
    matches!(
        field.data_type(),
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
    )
}

#[cfg(test)]
mod tests {
    use super::{StatValue, file_statistics, statistics_usable};
    use crate::{COLUMN_EVENT_TIME_UNIX_NANO, COLUMN_LOG_ATTRIBUTES, COLUMN_WAL_SEQUENCE};
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn bounds_cover_scalars_and_skip_json_and_non_finite_floats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(COLUMN_EVENT_TIME_UNIX_NANO, DataType::UInt64, false),
            Field::new(COLUMN_WAL_SEQUENCE, DataType::UInt64, false),
            Field::new("log_a_i64", DataType::Int64, true),
            Field::new(COLUMN_LOG_ATTRIBUTES, DataType::Utf8, false),
            Field::new("log_n_f64", DataType::Float64, true),
            Field::new("log_doc_json", DataType::Utf8, true).with_metadata(
                [(crate::FIELD_KIND.to_owned(), "json".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![5_u64, 8])),
                Arc::new(UInt64Array::from(vec![3_u64, 4])),
                Arc::new(Int64Array::from(vec![Some(4), None])),
                Arc::new(StringArray::from(vec!["{}", "{}"])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::NAN)])),
                Arc::new(StringArray::from(vec![Some("[]"), None])),
            ],
        )
        .expect("batch");
        let stats =
            file_statistics(schema.as_ref(), std::slice::from_ref(&batch), 12).expect("stats");
        assert_eq!(stats.rows, 2);
        assert_eq!(stats.event_time_min, 5);
        assert_eq!(stats.event_time_max, 8);
        assert_eq!(stats.wal_min, 3);
        assert_eq!(stats.wal_max, 4);
        let value = stats
            .columns
            .iter()
            .find(|column| column.name == "log_a_i64")
            .expect("value");
        assert_eq!(value.null_count, 1);
        assert_eq!(
            value.bounds.as_ref().map(|(min, max)| (min, max)),
            Some((&StatValue::Int64(4), &StatValue::Int64(4)))
        );
        assert!(
            stats
                .columns
                .iter()
                .find(|column| column.name == COLUMN_LOG_ATTRIBUTES)
                .expect("json")
                .bounds
                .is_none()
        );
        assert!(
            stats
                .columns
                .iter()
                .find(|column| column.name == "log_n_f64")
                .expect("float")
                .bounds
                .is_none()
        );
        assert!(
            stats
                .columns
                .iter()
                .find(|column| column.name == "log_doc_json")
                .expect("dynamic json")
                .bounds
                .is_none()
        );
        assert!(statistics_usable(
            &stats,
            schema.as_ref(),
            stats.rows,
            0,
            3_600_000_000_000,
            0,
            5
        ));
        let mut reversed = stats.clone();
        reversed.event_time_min = 9;
        assert!(!statistics_usable(
            &reversed,
            schema.as_ref(),
            reversed.rows,
            0,
            3_600_000_000_000,
            0,
            5
        ));
    }
}
