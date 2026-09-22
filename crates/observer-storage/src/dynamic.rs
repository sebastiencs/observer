//! Dynamic typed columns projected from OTLP attribute lists.
//!
//! Each admitted leaf becomes a nullable Arrow field. Nested key-value lists flatten until
//! [`DynamicLimits::max_depth`]; arrays and maps past that depth become one canonical JSON value.
//! Physical names are `{source}_{normalized_path}_{type}`. When different exact paths share a name,
//! every member of that admitted group gains a path digest suffix. Fields beyond
//! [`DynamicLimits::max_columns`] stay out of the projection. This module does not build
//! `RecordBatch` values and does not replace the canonical JSON attribute columns.

use std::{collections::BTreeSet, error::Error, fmt};

use arrow_schema::{DataType, Field};
use observer_protocol::otlp::{AnyValue, KeyValue, any_value};

use crate::{CanonicalJsonError, canonical_any_value_json};

/// Arrow field metadata key for [`AttributeSource::as_str`].
pub const FIELD_SOURCE: &str = "observer.attribute.source";

/// Arrow field metadata key for the original path as a JSON string array.
pub const FIELD_PATH: &str = "observer.attribute.path";

/// Arrow field metadata key for [`DynamicKind::as_str`].
pub const FIELD_KIND: &str = "observer.attribute.kind";

const DIGEST_PREFIX: usize = 8;

/// Which OTLP attribute list a dynamic field came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AttributeSource {
    /// Resource attributes.
    Resource,
    /// Instrumentation scope attributes.
    Scope,
    /// Log record attributes.
    Log,
}

impl AttributeSource {
    /// Stable source prefix used in physical names and field metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Scope => "scope",
            Self::Log => "log",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Resource => 1,
            Self::Scope => 2,
            Self::Log => 3,
        }
    }
}

/// Physical type of one dynamic leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DynamicKind {
    /// OTLP bool.
    Bool,
    /// OTLP int64.
    Int64,
    /// OTLP double, including non-finite values.
    Float64,
    /// OTLP string.
    String,
    /// OTLP bytes.
    Bytes,
    /// Canonical JSON for an array or a map at the depth boundary.
    Json,
}

impl DynamicKind {
    /// Stable type suffix used in physical names and field metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int64 => "i64",
            Self::Float64 => "f64",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Json => "json",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::String | Self::Json => DataType::Utf8,
            Self::Bytes => DataType::Binary,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::Bool => 1,
            Self::Int64 => 2,
            Self::Float64 => 3,
            Self::String => 4,
            Self::Bytes => 5,
            Self::Json => 6,
        }
    }
}

/// Exact identity of a dynamic leaf, before normalization.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DynamicIdentity {
    /// Attribute list that contained the leaf.
    pub source: AttributeSource,
    /// Original key segments, from the outer attribute to the leaf.
    pub path: Vec<String>,
    /// OTLP type stored for this path.
    pub kind: DynamicKind,
}

/// Typed value stored for one admitted leaf.
#[derive(Clone, Debug, PartialEq)]
pub enum DynamicValue {
    /// OTLP bool.
    Bool(bool),
    /// OTLP int64.
    Int64(i64),
    /// OTLP double.
    Float64(f64),
    /// OTLP string.
    String(String),
    /// OTLP bytes.
    Bytes(Vec<u8>),
    /// Canonical JSON text.
    Json(String),
}

impl DynamicValue {
    fn kind(&self) -> DynamicKind {
        match self {
            Self::Bool(_) => DynamicKind::Bool,
            Self::Int64(_) => DynamicKind::Int64,
            Self::Float64(_) => DynamicKind::Float64,
            Self::String(_) => DynamicKind::String,
            Self::Bytes(_) => DynamicKind::Bytes,
            Self::Json(_) => DynamicKind::Json,
        }
    }
}

/// One admitted dynamic column and the value found in this attribute set.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicColumn {
    /// Exact source, path, and type.
    pub identity: DynamicIdentity,
    /// Leaf value.
    pub value: DynamicValue,
    /// Normalized physical field name, including a digest suffix when names collide.
    pub physical_name: String,
}

impl DynamicColumn {
    /// Nullable Arrow field for this column.
    ///
    /// Metadata preserves the original source, path, and type.
    #[must_use]
    pub fn arrow_field(&self) -> Field {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            FIELD_SOURCE.to_owned(),
            self.identity.source.as_str().to_owned(),
        );
        metadata.insert(
            FIELD_PATH.to_owned(),
            serde_json::to_string(&self.identity.path).expect("attribute path is JSON"),
        );
        metadata.insert(
            FIELD_KIND.to_owned(),
            self.identity.kind.as_str().to_owned(),
        );
        Field::new(&self.physical_name, self.identity.kind.data_type(), true)
            .with_metadata(metadata)
    }
}

/// How far nested maps flatten, and how many dynamic columns a projection may admit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicLimits {
    /// Nested key-value lists entered before the remainder becomes one JSON leaf.
    ///
    /// Zero keeps every top-level map as JSON. One flattens a single map level.
    pub max_depth: usize,
    /// Maximum number of dynamic columns in the projection.
    pub max_columns: usize,
}

/// Admitted columns plus the identities that stayed in JSON because of the column cap.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicProjection {
    /// Columns ordered by physical name, then exact identity.
    pub columns: Vec<DynamicColumn>,
    /// Fields excluded by [`DynamicLimits::max_columns`], ordered by base name, then identity.
    pub overflow: Vec<DynamicIdentity>,
}

/// Why a dynamic projection could not be built.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicError {
    /// A JSON leaf was nested deeper than the canonical JSON encoder allows.
    CanonicalJson(CanonicalJsonError),
    /// Two different paths produced the same full digest.
    DigestCollision,
}

impl fmt::Display for DynamicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CanonicalJson(error) => write!(formatter, "{error}"),
            Self::DigestCollision => formatter.write_str("dynamic field digest collision"),
        }
    }
}

impl Error for DynamicError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CanonicalJson(error) => Some(error),
            Self::DigestCollision => None,
        }
    }
}

impl From<CanonicalJsonError> for DynamicError {
    fn from(error: CanonicalJsonError) -> Self {
        Self::CanonicalJson(error)
    }
}

/// Project attribute lists into bounded typed leaves.
///
/// Duplicate keys keep the last value, including nested keys. Empty values and profiling string
/// indexes produce no column. Collision suffixes are assigned only among admitted columns.
///
/// # Errors
///
/// Returns [`DynamicError::CanonicalJson`] when a JSON leaf exceeds the canonical encoder depth,
/// or [`DynamicError::DigestCollision`] when a full digest does not separate a collision group.
pub fn project_dynamic_fields<'a>(
    groups: impl IntoIterator<Item = (AttributeSource, &'a [KeyValue])>,
    limits: DynamicLimits,
) -> Result<DynamicProjection, DynamicError> {
    let mut collected = Vec::new();
    for (source, attributes) in groups {
        collect_attributes(&mut collected, source, attributes, &[], 0, limits.max_depth)?;
    }

    let mut pending: Vec<PendingLeaf> = collected
        .into_iter()
        .map(PendingLeaf::from_collected)
        .collect();
    pending.sort_by(|left, right| {
        left.base_name
            .cmp(&right.base_name)
            .then_with(|| left.identity.cmp(&right.identity))
    });
    let overflow = if pending.len() > limits.max_columns {
        pending.split_off(limits.max_columns)
    } else {
        Vec::new()
    };
    assign_physical_names(&mut pending)?;
    let mut columns: Vec<DynamicColumn> =
        pending.into_iter().map(PendingLeaf::into_column).collect();
    columns.sort_by(|left, right| {
        left.physical_name
            .cmp(&right.physical_name)
            .then_with(|| left.identity.cmp(&right.identity))
    });
    Ok(DynamicProjection {
        columns,
        overflow: overflow.into_iter().map(|leaf| leaf.identity).collect(),
    })
}

/// Core field names first, then dynamic columns in their projection order.
#[must_use]
pub fn ordered_field_names<'a>(core: &'a [&'a str], columns: &'a [DynamicColumn]) -> Vec<&'a str> {
    core.iter()
        .copied()
        .chain(columns.iter().map(|column| column.physical_name.as_str()))
        .collect()
}

struct Collected {
    source: AttributeSource,
    path: Vec<String>,
    value: DynamicValue,
}

struct PendingLeaf {
    identity: DynamicIdentity,
    value: DynamicValue,
    base_name: String,
    physical_name: String,
}

impl PendingLeaf {
    fn from_collected(leaf: Collected) -> Self {
        let identity = DynamicIdentity {
            source: leaf.source,
            path: leaf.path,
            kind: leaf.value.kind(),
        };
        let base_name = base_name(&identity);
        Self {
            identity,
            value: leaf.value,
            base_name,
            physical_name: String::new(),
        }
    }

    fn into_column(self) -> DynamicColumn {
        DynamicColumn {
            identity: self.identity,
            value: self.value,
            physical_name: self.physical_name,
        }
    }
}

fn collect_attributes(
    leaves: &mut Vec<Collected>,
    source: AttributeSource,
    attributes: &[KeyValue],
    path: &[String],
    depth: usize,
    max_depth: usize,
) -> Result<(), DynamicError> {
    for attribute in attributes {
        let mut child = Vec::with_capacity(path.len() + 1);
        child.extend_from_slice(path);
        child.push(attribute.key.clone());
        clear_prefix(leaves, source, &child);
        if let Some(value) = &attribute.value {
            project_value(leaves, source, &child, value, depth, max_depth)?;
        }
    }
    Ok(())
}

fn project_value(
    leaves: &mut Vec<Collected>,
    source: AttributeSource,
    path: &[String],
    value: &AnyValue,
    depth: usize,
    max_depth: usize,
) -> Result<(), DynamicError> {
    let Some(kind) = &value.value else {
        return Ok(());
    };
    match kind {
        any_value::Value::StringValueStrindex(_) => Ok(()),
        any_value::Value::StringValue(text) => {
            push(leaves, source, path, DynamicValue::String(text.clone()));
            Ok(())
        }
        any_value::Value::BoolValue(flag) => {
            push(leaves, source, path, DynamicValue::Bool(*flag));
            Ok(())
        }
        any_value::Value::IntValue(integer) => {
            push(leaves, source, path, DynamicValue::Int64(*integer));
            Ok(())
        }
        any_value::Value::DoubleValue(number) => {
            push(leaves, source, path, DynamicValue::Float64(*number));
            Ok(())
        }
        any_value::Value::BytesValue(bytes) => {
            push(leaves, source, path, DynamicValue::Bytes(bytes.clone()));
            Ok(())
        }
        any_value::Value::ArrayValue(_) => {
            push(
                leaves,
                source,
                path,
                DynamicValue::Json(canonical_any_value_json(value)?),
            );
            Ok(())
        }
        any_value::Value::KvlistValue(list) => {
            if depth >= max_depth {
                push(
                    leaves,
                    source,
                    path,
                    DynamicValue::Json(canonical_any_value_json(value)?),
                );
                Ok(())
            } else {
                collect_attributes(leaves, source, &list.values, path, depth + 1, max_depth)
            }
        }
    }
}

fn push(
    leaves: &mut Vec<Collected>,
    source: AttributeSource,
    path: &[String],
    value: DynamicValue,
) {
    leaves.push(Collected {
        source,
        path: path.to_vec(),
        value,
    });
}

fn clear_prefix(leaves: &mut Vec<Collected>, source: AttributeSource, path: &[String]) {
    leaves.retain(|leaf| leaf.source != source || !leaf.path.starts_with(path));
}

fn base_name(identity: &DynamicIdentity) -> String {
    let mut name = String::from(identity.source.as_str());
    for segment in &identity.path {
        name.push('_');
        name.push_str(&normalize_segment(segment));
    }
    name.push('_');
    name.push_str(identity.kind.as_str());
    name
}

fn normalize_segment(segment: &str) -> String {
    let mut normalized = String::with_capacity(segment.len());
    for character in segment.chars() {
        for lower in character.to_lowercase() {
            if lower.is_ascii_alphanumeric() {
                normalized.push(lower);
            } else {
                normalized.push('_');
            }
        }
    }
    normalized
}

fn assign_physical_names(leaves: &mut [PendingLeaf]) -> Result<(), DynamicError> {
    let mut index = 0;
    while index < leaves.len() {
        let base = leaves[index].base_name.clone();
        let mut end = index + 1;
        while end < leaves.len() && leaves[end].base_name == base {
            end += 1;
        }
        if end == index + 1 {
            leaves[index].physical_name.clone_from(&base);
        } else {
            let digests: Vec<String> = leaves[index..end]
                .iter()
                .map(|leaf| identity_digest(&leaf.identity))
                .collect();
            let width = unique_hex_width(&digests)?;
            for (leaf, digest) in leaves[index..end].iter_mut().zip(digests) {
                leaf.physical_name = format!("{base}__{}", &digest[..width]);
            }
        }
        index = end;
    }
    Ok(())
}

fn identity_digest(identity: &DynamicIdentity) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[identity.source.tag()]);
    let count = u64::try_from(identity.path.len()).expect("attribute path length");
    hasher.update(&count.to_be_bytes());
    for segment in &identity.path {
        let length = u64::try_from(segment.len()).expect("attribute key length");
        hasher.update(&length.to_be_bytes());
        hasher.update(segment.as_bytes());
    }
    hasher.update(&[identity.kind.tag()]);
    hex_encode(hasher.finalize().as_bytes())
}

fn unique_hex_width(digests: &[String]) -> Result<usize, DynamicError> {
    let full = digests.iter().map(String::len).min().unwrap_or(0);
    let mut width = DIGEST_PREFIX.min(full);
    if width == 0 {
        return Err(DynamicError::DigestCollision);
    }
    loop {
        if prefixes_unique(digests, width) {
            return Ok(width);
        }
        if width == full {
            return Err(DynamicError::DigestCollision);
        }
        width = full.min(width.saturating_add(DIGEST_PREFIX));
    }
}

fn prefixes_unique(digests: &[String], width: usize) -> bool {
    let mut seen = BTreeSet::new();
    digests.iter().all(|digest| {
        digest
            .get(..width)
            .is_some_and(|prefix| seen.insert(prefix.to_owned()))
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{
        AttributeSource, DynamicError, DynamicKind, DynamicLimits, DynamicValue, FIELD_KIND,
        FIELD_PATH, FIELD_SOURCE, ordered_field_names, project_dynamic_fields, unique_hex_width,
    };
    use crate::{COLUMN_BODY, COLUMN_TENANT_ID, CanonicalJsonError, MAX_JSON_DEPTH};
    use arrow_schema::DataType;
    use observer_protocol::otlp::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value};
    use proptest::prelude::*;

    fn limits(max_depth: usize, max_columns: usize) -> DynamicLimits {
        DynamicLimits {
            max_depth,
            max_columns,
        }
    }

    fn otlp_any(value: any_value::Value) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    fn attribute(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(otlp_any(value)),
            ..Default::default()
        }
    }

    fn int_attribute(key: &str, value: i64) -> KeyValue {
        attribute(key, any_value::Value::IntValue(value))
    }

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        attribute(key, any_value::Value::StringValue(value.to_owned()))
    }

    fn kvlist(values: Vec<KeyValue>) -> any_value::Value {
        any_value::Value::KvlistValue(KeyValueList { values })
    }

    fn project_log(attributes: &[KeyValue], max_depth: usize) -> super::DynamicProjection {
        project_dynamic_fields([(AttributeSource::Log, attributes)], limits(max_depth, 32))
            .expect("projection")
    }

    fn column<'a>(
        projection: &'a super::DynamicProjection,
        path: &[&str],
    ) -> &'a super::DynamicColumn {
        projection
            .columns
            .iter()
            .find(|column| {
                column
                    .identity
                    .path
                    .iter()
                    .map(String::as_str)
                    .eq(path.iter().copied())
            })
            .expect("column")
    }

    #[test]
    fn sources_and_dotted_keys_stay_distinct_from_nested_paths() {
        let attributes = [int_attribute("http.status", 500)];
        let nested = [attribute(
            "http",
            kvlist(vec![int_attribute("status", 200)]),
        )];
        let resource = project_dynamic_fields(
            [
                (AttributeSource::Resource, attributes.as_slice()),
                (AttributeSource::Scope, attributes.as_slice()),
                (AttributeSource::Log, nested.as_slice()),
            ],
            limits(4, 32),
        )
        .expect("projection");

        assert_eq!(
            column(&resource, &["http.status"]).physical_name,
            "resource_http_status_i64"
        );
        assert!(
            resource
                .columns
                .iter()
                .any(|column| column.physical_name == "scope_http_status_i64")
        );
        assert_eq!(
            column(&resource, &["http", "status"]).physical_name,
            "log_http_status_i64"
        );
        assert_eq!(
            column(&resource, &["http", "status"]).value,
            DynamicValue::Int64(200)
        );
    }

    #[test]
    fn dotted_and_nested_paths_that_normalize_together_all_receive_suffixes() {
        let attributes = [
            int_attribute("http.status", 1),
            attribute("http", kvlist(vec![int_attribute("status", 2)])),
        ];
        let projection = project_log(&attributes, 4);
        assert_eq!(projection.columns.len(), 2);
        for column in &projection.columns {
            assert!(column.physical_name.starts_with("log_http_status_i64__"));
            assert_eq!(
                column.physical_name.len(),
                "log_http_status_i64__".len() + 8
            );
        }
        assert_ne!(
            projection.columns[0].physical_name,
            projection.columns[1].physical_name
        );
        assert_eq!(
            column(&projection, &["http.status"]).value,
            DynamicValue::Int64(1)
        );
        assert_eq!(
            column(&projection, &["http", "status"]).value,
            DynamicValue::Int64(2)
        );
    }

    #[test]
    fn normalization_collisions_keep_both_values() {
        let attributes = [
            int_attribute("HTTP-Status", 1),
            int_attribute("http_status", 2),
        ];
        let projection = project_log(&attributes, 4);
        assert_eq!(
            column(&projection, &["HTTP-Status"]).value,
            DynamicValue::Int64(1)
        );
        assert_eq!(
            column(&projection, &["http_status"]).value,
            DynamicValue::Int64(2)
        );
        assert!(
            projection
                .columns
                .iter()
                .all(|column| { column.physical_name.starts_with("log_http_status_i64__") })
        );
    }

    #[test]
    fn scalars_arrays_and_absent_values_map_to_typed_leaves() {
        let attributes = [
            string_attribute("message", "hello"),
            attribute("ok", any_value::Value::BoolValue(true)),
            int_attribute("status", 500),
            attribute("latency", any_value::Value::DoubleValue(1.5)),
            attribute("nan", any_value::Value::DoubleValue(f64::NAN)),
            attribute("payload", any_value::Value::BytesValue(b"hi".to_vec())),
            attribute(
                "items",
                any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        otlp_any(any_value::Value::IntValue(1)),
                        otlp_any(any_value::Value::StringValue("x".to_owned())),
                    ],
                }),
            ),
            KeyValue {
                key: "missing".to_owned(),
                value: None,
                ..Default::default()
            },
            KeyValue {
                key: "empty".to_owned(),
                value: Some(AnyValue { value: None }),
                ..Default::default()
            },
            attribute("indexed", any_value::Value::StringValueStrindex(3)),
        ];
        let projection = project_log(&attributes, 4);
        assert_eq!(
            column(&projection, &["message"]).value,
            DynamicValue::String("hello".to_owned())
        );
        assert_eq!(column(&projection, &["ok"]).value, DynamicValue::Bool(true));
        assert_eq!(
            column(&projection, &["status"]).value,
            DynamicValue::Int64(500)
        );
        assert_eq!(
            column(&projection, &["latency"]).value,
            DynamicValue::Float64(1.5)
        );
        match &column(&projection, &["nan"]).value {
            DynamicValue::Float64(number) => assert!(number.is_nan()),
            other => panic!("expected nan, got {other:?}"),
        }
        assert_eq!(
            column(&projection, &["payload"]).value,
            DynamicValue::Bytes(b"hi".to_vec())
        );
        assert_eq!(
            column(&projection, &["items"]).value,
            DynamicValue::Json("[1,\"x\"]".to_owned())
        );
        assert_eq!(
            column(&projection, &["items"]).identity.kind,
            DynamicKind::Json
        );
        assert!(projection.columns.iter().all(|column| {
            !matches!(
                column.identity.path.first().map(String::as_str),
                Some("missing" | "empty" | "indexed")
            )
        }));
        assert_eq!(
            column(&projection, &["status"]).physical_name,
            "log_status_i64"
        );
        assert_eq!(
            column(&projection, &["message"]).physical_name,
            "log_message_string"
        );
        assert_eq!(column(&projection, &["ok"]).physical_name, "log_ok_bool");
        assert_eq!(
            column(&projection, &["latency"]).physical_name,
            "log_latency_f64"
        );
        assert_eq!(
            column(&projection, &["payload"]).physical_name,
            "log_payload_bytes"
        );
        assert_eq!(
            column(&projection, &["items"]).physical_name,
            "log_items_json"
        );
    }

    #[test]
    fn arrow_metadata_preserves_the_original_path() {
        let projection = project_log(&[int_attribute("http.status", 500)], 4);
        let field = projection.columns[0].arrow_field();
        assert_eq!(field.name(), "log_http_status_i64");
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(field.is_nullable());
        assert_eq!(
            field.metadata().get(FIELD_SOURCE).map(String::as_str),
            Some("log")
        );
        assert_eq!(
            field.metadata().get(FIELD_KIND).map(String::as_str),
            Some("i64")
        );
        assert_eq!(
            field.metadata().get(FIELD_PATH).map(String::as_str),
            Some(r#"["http.status"]"#)
        );
    }

    #[test]
    fn depth_boundary_emits_one_json_leaf_and_deeper_limits_flatten() {
        let attributes = [attribute(
            "http",
            kvlist(vec![
                int_attribute("status", 500),
                attribute("request", kvlist(vec![string_attribute("id", "abc")])),
            ]),
        )];

        let stopped = project_log(&attributes, 0);
        assert_eq!(stopped.columns.len(), 1);
        assert_eq!(stopped.columns[0].physical_name, "log_http_json");
        assert_eq!(
            stopped.columns[0].value,
            DynamicValue::Json(r#"{"request":{"id":"abc"},"status":500}"#.to_owned())
        );

        let one_level = project_log(&attributes, 1);
        assert_eq!(
            column(&one_level, &["http", "status"]).value,
            DynamicValue::Int64(500)
        );
        assert_eq!(
            column(&one_level, &["http", "request"]).value,
            DynamicValue::Json(r#"{"id":"abc"}"#.to_owned())
        );
        assert_eq!(
            column(&one_level, &["http", "request"]).physical_name,
            "log_http_request_json"
        );

        let two_levels = project_log(&attributes, 2);
        assert_eq!(
            column(&two_levels, &["http", "request", "id"]).value,
            DynamicValue::String("abc".to_owned())
        );
        assert_eq!(
            column(&two_levels, &["http", "request", "id"]).physical_name,
            "log_http_request_id_string"
        );
    }

    #[test]
    fn duplicate_keys_keep_the_last_value_and_drop_replaced_children() {
        let attributes = [
            int_attribute("status", 1),
            string_attribute("status", "ok"),
            attribute("http", kvlist(vec![int_attribute("status", 200)])),
            int_attribute("http", 7),
        ];
        let projection = project_log(&attributes, 4);
        assert_eq!(projection.columns.len(), 2);
        assert_eq!(
            column(&projection, &["status"]).value,
            DynamicValue::String("ok".to_owned())
        );
        assert_eq!(column(&projection, &["http"]).value, DynamicValue::Int64(7));
    }

    #[test]
    fn column_cap_keeps_the_first_names_and_reports_overflow() {
        let attributes = [
            int_attribute("c", 3),
            int_attribute("a", 1),
            int_attribute("b", 2),
        ];
        let projection = project_dynamic_fields(
            [(AttributeSource::Log, attributes.as_slice())],
            limits(4, 2),
        )
        .expect("projection");
        let names: Vec<_> = projection
            .columns
            .iter()
            .map(|column| column.physical_name.as_str())
            .collect();
        assert_eq!(names, ["log_a_i64", "log_b_i64"]);
        assert_eq!(projection.overflow.len(), 1);
        assert_eq!(projection.overflow[0].path, ["c"]);

        let none = project_dynamic_fields(
            [(AttributeSource::Log, attributes.as_slice())],
            limits(4, 0),
        )
        .expect("empty cap");
        assert!(none.columns.is_empty());
        assert_eq!(none.overflow.len(), 3);
    }

    #[test]
    fn names_are_deterministic_and_core_columns_stay_first() {
        let attributes = [
            int_attribute("HTTP-Status", 1),
            int_attribute("http_status", 2),
        ];
        let first = project_log(&attributes, 4);
        let second = project_log(&attributes, 4);
        assert_eq!(first, second);
        let names = ordered_field_names(&[COLUMN_TENANT_ID, COLUMN_BODY], &first.columns);
        assert_eq!(
            names,
            vec![
                COLUMN_TENANT_ID,
                COLUMN_BODY,
                first.columns[0].physical_name.as_str(),
                first.columns[1].physical_name.as_str(),
            ]
        );
    }

    #[test]
    fn json_leaves_deeper_than_the_encoder_allow_fail() {
        let mut value = otlp_any(any_value::Value::IntValue(1));
        for _ in 0..MAX_JSON_DEPTH {
            value = otlp_any(any_value::Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "nested".to_owned(),
                    value: Some(value),
                    ..Default::default()
                }],
            }));
        }
        let attributes = [KeyValue {
            key: "body".to_owned(),
            value: Some(value),
            ..Default::default()
        }];
        let error = project_dynamic_fields(
            [(AttributeSource::Log, attributes.as_slice())],
            limits(0, 8),
        );
        assert_eq!(
            error,
            Err(DynamicError::CanonicalJson(
                CanonicalJsonError::NestingTooDeep
            ))
        );
    }

    #[test]
    fn digest_prefixes_widen_until_they_differ() {
        let shared = "01234567aaaaaaaa".to_owned();
        let other = "01234567bbbbbbbb".to_owned();
        assert_eq!(unique_hex_width(&[shared, other]).expect("width"), 16);
        let same = "0123456789abcdef".to_owned();
        assert_eq!(
            unique_hex_width(&[same.clone(), same]),
            Err(DynamicError::DigestCollision)
        );
    }

    proptest! {
        #[test]
        fn repeated_keys_keep_the_last_integer(values in prop::collection::vec(any::<i64>(), 1..12)) {
            let attributes: Vec<_> = values.iter().copied().map(|value| int_attribute("status", value)).collect();
            let projection = project_log(&attributes, 2);
            assert_eq!(projection.columns.len(), 1);
            assert_eq!(projection.columns[0].value, DynamicValue::Int64(*values.last().expect("value")));
            assert_eq!(projection.columns[0].physical_name, "log_status_i64");
        }
    }
}
