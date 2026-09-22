//! Lean Arrow schema for one OTLP log row.

use std::sync::OnceLock;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Schema version stamped on every decoded row.
pub const SCHEMA_VERSION: u16 = 1;

/// Resource attribute promoted into [`COLUMN_SERVICE_NAME`].
pub const SERVICE_NAME_ATTRIBUTE: &str = "service.name";

/// Byte width of an OTLP trace id.
pub const TRACE_ID_BYTES: usize = 16;

/// Byte width of an OTLP span id.
pub const SPAN_ID_BYTES: usize = 8;

/// Arrow fixed-size binary width of an OTLP trace id.
pub const TRACE_ID_LEN: i32 = TRACE_ID_BYTES as i32;

/// Arrow fixed-size binary width of an OTLP span id.
pub const SPAN_ID_LEN: i32 = SPAN_ID_BYTES as i32;

pub const COLUMN_SCHEMA_VERSION: &str = "schema_version";
pub const COLUMN_TENANT_ID: &str = "tenant_id";
pub const COLUMN_WAL_SEQUENCE: &str = "wal_sequence";
pub const COLUMN_RECORD_INDEX: &str = "record_index";
pub const COLUMN_TIME_UNIX_NANO: &str = "time_unix_nano";
pub const COLUMN_OBSERVED_TIME_UNIX_NANO: &str = "observed_time_unix_nano";
pub const COLUMN_RECEIVED_TIME_UNIX_NANO: &str = "received_time_unix_nano";
pub const COLUMN_EVENT_TIME_UNIX_NANO: &str = "event_time_unix_nano";
pub const COLUMN_SEVERITY_NUMBER: &str = "severity_number";
pub const COLUMN_SEVERITY_TEXT: &str = "severity_text";
pub const COLUMN_BODY: &str = "body";
pub const COLUMN_TRACE_ID: &str = "trace_id";
pub const COLUMN_SPAN_ID: &str = "span_id";
pub const COLUMN_SERVICE_NAME: &str = "service_name";
pub const COLUMN_RESOURCE_ATTRIBUTES: &str = "resource_attributes";
pub const COLUMN_SCOPE_ATTRIBUTES: &str = "scope_attributes";
pub const COLUMN_LOG_ATTRIBUTES: &str = "log_attributes";

const NANOS_PER_SECOND: u64 = 1_000_000_000;
const SECONDS_PER_HOUR: u64 = 3_600;
const SECONDS_PER_DAY: u64 = 86_400;

/// UTC hour that contains a derived event timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EventHour {
    start_unix_nano: u64,
}

impl EventHour {
    /// Hour containing `unix_nano`, aligned to the start of that UTC hour.
    #[must_use]
    pub fn containing(unix_nano: u64) -> Self {
        let nanos_per_hour = SECONDS_PER_HOUR * NANOS_PER_SECOND;
        Self {
            start_unix_nano: unix_nano - unix_nano % nanos_per_hour,
        }
    }

    #[must_use]
    pub fn start_unix_nano(self) -> u64 {
        self.start_unix_nano
    }

    /// Civil year, month, and day of this hour in UTC.
    #[must_use]
    pub fn utc_date(self) -> (i32, u32, u32) {
        let seconds = self.start_unix_nano / NANOS_PER_SECOND;
        civil_from_days(i64::try_from(seconds / SECONDS_PER_DAY).expect("unix day count"))
    }

    /// Hour of the day in UTC, from 0 through 23.
    #[must_use]
    pub fn utc_hour(self) -> u8 {
        let seconds = self.start_unix_nano / NANOS_PER_SECOND;
        u8::try_from((seconds / SECONDS_PER_HOUR) % 24).expect("hour")
    }
}

/// Stable core and JSON fallback columns.
///
/// Dynamic attribute columns are appended by [`logs_batch_schema`]. A batch schema matches this
/// value only when the frame admitted no dynamic fields.
#[must_use]
pub fn logs_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| std::sync::Arc::new(Schema::new(schema_fields())))
        .clone()
}

/// Core columns followed by the frame's admitted dynamic fields.
#[must_use]
pub fn logs_batch_schema(dynamic: &[Field]) -> SchemaRef {
    if dynamic.is_empty() {
        return logs_schema();
    }
    let mut fields = schema_fields();
    fields.extend(dynamic.iter().cloned());
    std::sync::Arc::new(Schema::new(fields))
}

fn schema_fields() -> Vec<Field> {
    vec![
        field(COLUMN_SCHEMA_VERSION, DataType::UInt16, false),
        field(COLUMN_TENANT_ID, DataType::Utf8, false),
        field(COLUMN_WAL_SEQUENCE, DataType::UInt64, false),
        field(COLUMN_RECORD_INDEX, DataType::UInt32, false),
        field(COLUMN_TIME_UNIX_NANO, DataType::UInt64, true),
        field(COLUMN_OBSERVED_TIME_UNIX_NANO, DataType::UInt64, true),
        field(COLUMN_RECEIVED_TIME_UNIX_NANO, DataType::UInt64, false),
        field(COLUMN_EVENT_TIME_UNIX_NANO, DataType::UInt64, false),
        field(COLUMN_SEVERITY_NUMBER, DataType::Int32, true),
        field(COLUMN_SEVERITY_TEXT, DataType::Utf8, true),
        field(COLUMN_BODY, DataType::Utf8, true),
        field(
            COLUMN_TRACE_ID,
            DataType::FixedSizeBinary(TRACE_ID_LEN),
            true,
        ),
        field(COLUMN_SPAN_ID, DataType::FixedSizeBinary(SPAN_ID_LEN), true),
        field(COLUMN_SERVICE_NAME, DataType::Utf8, true),
        field(COLUMN_RESOURCE_ATTRIBUTES, DataType::Utf8, false),
        field(COLUMN_SCOPE_ATTRIBUTES, DataType::Utf8, false),
        field(COLUMN_LOG_ATTRIBUTES, DataType::Utf8, false),
    ]
}

fn field(name: &'static str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable)
}

/// Howard Hinnant's civil-from-days conversion.
///
/// `days` is the number of days since 1970-01-01. The result is `(year, month, day)`.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = u64::try_from(z - era * 146_097).expect("day of era");
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = i64::try_from(year_of_era).expect("year of era") + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = if month_part < 10 {
        month_part + 3
    } else {
        month_part - 9
    };
    if month <= 2 {
        year += 1;
    }
    (
        i32::try_from(year).expect("year"),
        u32::try_from(month).expect("month"),
        u32::try_from(day).expect("day"),
    )
}

#[cfg(test)]
mod tests {
    use super::{EventHour, logs_batch_schema, logs_schema, schema_fields};
    use arrow_schema::{DataType, Field};

    fn nanos(seconds: u64) -> u64 {
        seconds * 1_000_000_000
    }

    #[test]
    fn logs_schema_v1_columns() {
        let schema = logs_schema();
        let fields = schema_fields();
        assert_eq!(schema.fields().len(), fields.len());
        for (index, expected) in fields.iter().enumerate() {
            let field = schema.field(index);
            assert_eq!(field.name(), expected.name());
            assert_eq!(field.data_type(), expected.data_type());
            assert_eq!(field.is_nullable(), expected.is_nullable());
        }
    }

    #[test]
    fn batch_schema_keeps_core_fields_and_appends_dynamic_ones() {
        assert!(std::sync::Arc::ptr_eq(
            &logs_batch_schema(&[]),
            &logs_schema()
        ));
        let extra = Field::new("log_status_i64", DataType::Int64, true);
        let schema = logs_batch_schema(std::slice::from_ref(&extra));
        assert_eq!(schema.fields().len(), logs_schema().fields().len() + 1);
        assert_eq!(
            schema.fields().last().map(std::convert::AsRef::as_ref),
            Some(&extra)
        );
        for (index, field) in logs_schema().fields().iter().enumerate() {
            assert_eq!(schema.field(index), field.as_ref());
        }
    }

    #[test]
    fn utc_hours_cover_epoch_leap_days_and_boundaries() {
        let cases = [
            (0, (1970, 1, 1), 0),
            (1_699_999_200, (2023, 11, 14), 22),
            (951_782_400, (2000, 2, 29), 0),
            (1_709_164_800, (2024, 2, 29), 0),
            (1_704_067_200, (2024, 1, 1), 0),
        ];
        for (seconds, date, hour) in cases {
            let event_hour = EventHour::containing(nanos(seconds));
            assert_eq!(event_hour.start_unix_nano(), nanos(seconds));
            assert_eq!(event_hour.utc_date(), date);
            assert_eq!(event_hour.utc_hour(), hour);
        }

        let before_new_year = EventHour::containing(nanos(1_704_067_200) - 1);
        assert_eq!(before_new_year.utc_date(), (2023, 12, 31));
        assert_eq!(before_new_year.utc_hour(), 23);
        assert_eq!(before_new_year.start_unix_nano(), nanos(1_704_063_600));
    }
}
