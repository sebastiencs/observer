//! Canonical JSON for OTLP `AnyValue` and attribute lists.
//!
//! Objects sort keys by UTF-8 byte order and keep the last value when a key repeats.
//! Scalars, arrays, and maps stay native JSON values. Bytes become `{"$bytes":"<base64>"}`.
//! Non-finite floats become `{"$float":"NaN"}`, `{"$float":"Infinity"}`, or
//! `{"$float":"-Infinity"}`. Empty values and profiling string indexes become JSON null.

use std::{collections::BTreeMap, error::Error, fmt};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use observer_protocol::otlp::{AnyValue, KeyValue, any_value};
use serde_json::{Number, Value};

/// Maximum nested `AnyValue` depth accepted by the encoder.
pub const MAX_JSON_DEPTH: usize = 64;

/// Why an OTLP value could not be encoded as canonical JSON.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalJsonError {
    /// Arrays or maps are nested deeper than [`MAX_JSON_DEPTH`].
    NestingTooDeep,
}

impl fmt::Display for CanonicalJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NestingTooDeep => formatter.write_str("OTLP value nesting is too deep"),
        }
    }
}

impl Error for CanonicalJsonError {}

/// Encode an attribute list as a compact canonical JSON object.
pub fn canonical_attributes_json(attributes: &[KeyValue]) -> Result<String, CanonicalJsonError> {
    Ok(to_json(&attributes_value(attributes, 0)?))
}

/// Encode one `AnyValue` as compact canonical JSON.
pub fn canonical_any_value_json(value: &AnyValue) -> Result<String, CanonicalJsonError> {
    Ok(to_json(&any_value_to_json(value, 0)?))
}

fn attributes_value(attributes: &[KeyValue], depth: usize) -> Result<Value, CanonicalJsonError> {
    let mut object = BTreeMap::new();
    for attribute in attributes {
        object.insert(
            attribute.key.clone(),
            optional_any_value(attribute.value.as_ref(), depth)?,
        );
    }
    Ok(Value::Object(object.into_iter().collect()))
}

fn optional_any_value(value: Option<&AnyValue>, depth: usize) -> Result<Value, CanonicalJsonError> {
    value.map_or(Ok(Value::Null), |value| any_value_to_json(value, depth))
}

fn any_value_to_json(value: &AnyValue, depth: usize) -> Result<Value, CanonicalJsonError> {
    if depth >= MAX_JSON_DEPTH {
        return Err(CanonicalJsonError::NestingTooDeep);
    }
    match &value.value {
        None | Some(any_value::Value::StringValueStrindex(_)) => Ok(Value::Null),
        Some(any_value::Value::StringValue(text)) => Ok(Value::String(text.clone())),
        Some(any_value::Value::BoolValue(flag)) => Ok(Value::Bool(*flag)),
        Some(any_value::Value::IntValue(integer)) => Ok(Value::Number((*integer).into())),
        Some(any_value::Value::DoubleValue(number)) => Ok(float_value(*number)),
        Some(any_value::Value::ArrayValue(array)) => {
            let mut items = Vec::with_capacity(array.values.len());
            for item in &array.values {
                items.push(any_value_to_json(item, depth + 1)?);
            }
            Ok(Value::Array(items))
        }
        Some(any_value::Value::KvlistValue(list)) => attributes_value(&list.values, depth + 1),
        Some(any_value::Value::BytesValue(bytes)) => {
            Ok(tagged_string("$bytes", &STANDARD.encode(bytes)))
        }
    }
}

fn float_value(number: f64) -> Value {
    if number.is_nan() {
        tagged_string("$float", "NaN")
    } else if number.is_infinite() {
        tagged_string(
            "$float",
            if number.is_sign_positive() {
                "Infinity"
            } else {
                "-Infinity"
            },
        )
    } else {
        Value::Number(Number::from_f64(number).expect("finite float"))
    }
}

fn tagged_string(tag: &str, value: &str) -> Value {
    let mut object = BTreeMap::new();
    object.insert(tag.to_owned(), Value::String(value.to_owned()));
    Value::Object(object.into_iter().collect())
}

fn to_json(value: &Value) -> String {
    serde_json::to_string(value).expect("canonical JSON values are finite")
}

#[cfg(test)]
mod tests {
    use super::{
        CanonicalJsonError, MAX_JSON_DEPTH, canonical_any_value_json, canonical_attributes_json,
    };
    use observer_protocol::otlp::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value};
    use proptest::prelude::*;
    use serde_json::{Number, Value};
    use std::collections::BTreeMap;

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

    fn nested_arrays(depth: usize) -> AnyValue {
        let mut value = otlp_any(any_value::Value::IntValue(1));
        for _ in 0..depth {
            value = otlp_any(any_value::Value::ArrayValue(ArrayValue {
                values: vec![value],
            }));
        }
        value
    }

    #[test]
    fn objects_sort_keys_and_keep_the_last_duplicate() {
        let json = canonical_attributes_json(&[
            int_attribute("b", 1),
            int_attribute("a", 1),
            int_attribute("a", 2),
        ])
        .expect("attributes");
        assert_eq!(json, r#"{"a":2,"b":1}"#);
    }

    #[test]
    fn scalars_arrays_maps_and_tagged_bytes_keep_their_json_types() {
        let body = otlp_any(any_value::Value::KvlistValue(KeyValueList {
            values: vec![
                attribute("z", any_value::Value::BoolValue(true)),
                attribute(
                    "a",
                    any_value::Value::ArrayValue(ArrayValue {
                        values: vec![
                            otlp_any(any_value::Value::IntValue(1)),
                            otlp_any(any_value::Value::StringValue("x".to_owned())),
                            AnyValue { value: None },
                            otlp_any(any_value::Value::BytesValue(b"hi".to_vec())),
                        ],
                    }),
                ),
            ],
        }));
        assert_eq!(
            canonical_any_value_json(&body).expect("value"),
            r#"{"a":[1,"x",null,{"$bytes":"aGk="}],"z":true}"#
        );
    }

    #[test]
    fn non_finite_floats_and_empty_values_are_tagged_or_null() {
        assert_eq!(
            canonical_any_value_json(&otlp_any(any_value::Value::DoubleValue(f64::NAN)))
                .expect("nan"),
            r#"{"$float":"NaN"}"#
        );
        assert_eq!(
            canonical_any_value_json(&otlp_any(any_value::Value::DoubleValue(f64::INFINITY)))
                .expect("infinity"),
            r#"{"$float":"Infinity"}"#
        );
        assert_eq!(
            canonical_any_value_json(&otlp_any(any_value::Value::DoubleValue(f64::NEG_INFINITY)))
                .expect("negative infinity"),
            r#"{"$float":"-Infinity"}"#
        );
        assert_eq!(
            canonical_attributes_json(&[KeyValue {
                key: "missing".to_owned(),
                value: None,
                ..Default::default()
            }])
            .expect("missing"),
            r#"{"missing":null}"#
        );
        assert_eq!(
            canonical_any_value_json(&otlp_any(any_value::Value::StringValueStrindex(3)))
                .expect("index"),
            "null"
        );
        assert_eq!(
            canonical_any_value_json(&otlp_any(any_value::Value::BytesValue(Vec::new())))
                .expect("empty"),
            r#"{"$bytes":""}"#
        );
    }

    #[test]
    fn quoted_and_unicode_keys_follow_serde_json() {
        let attributes = [
            int_attribute("a\"b", 1),
            int_attribute("é", 2),
            int_attribute("a", 3),
        ];
        let mut expected = BTreeMap::new();
        expected.insert("a\"b".to_owned(), Value::from(1));
        expected.insert("é".to_owned(), Value::from(2));
        expected.insert("a".to_owned(), Value::from(3));
        let expected = serde_json::to_string(&Value::Object(expected.into_iter().collect()))
            .expect("expected json");
        assert_eq!(
            canonical_attributes_json(&attributes).expect("attributes"),
            expected
        );
    }

    #[test]
    fn nesting_limit_rejects_one_level_past_the_maximum() {
        assert!(canonical_any_value_json(&nested_arrays(MAX_JSON_DEPTH - 1)).is_ok());
        assert_eq!(
            canonical_any_value_json(&nested_arrays(MAX_JSON_DEPTH)),
            Err(CanonicalJsonError::NestingTooDeep)
        );
    }

    proptest! {
        #[test]
        fn attribute_maps_match_sorted_last_wins(
            pairs in prop::collection::vec(("[a-z]{0,6}", any::<i64>()), 0..20)
        ) {
            let attributes: Vec<_> = pairs
                .iter()
                .map(|(key, value)| int_attribute(key, *value))
                .collect();
            let mut expected = BTreeMap::new();
            for (key, value) in &pairs {
                expected.insert(key.clone(), Value::from(*value));
            }
            let expected = serde_json::to_string(&Value::Object(expected.into_iter().collect()))
                .unwrap();
            assert_eq!(canonical_attributes_json(&attributes).unwrap(), expected);
        }

        #[test]
        fn arrays_preserve_order(values in prop::collection::vec(any::<i64>(), 0..16)) {
            let value = otlp_any(any_value::Value::ArrayValue(ArrayValue {
                values: values
                    .iter()
                    .copied()
                    .map(|value| otlp_any(any_value::Value::IntValue(value)))
                    .collect(),
            }));
            let expected = Value::Array(values.into_iter().map(Value::from).collect());
            assert_eq!(
                canonical_any_value_json(&value).unwrap(),
                serde_json::to_string(&expected).unwrap()
            );
        }

        #[test]
        fn finite_doubles_match_json_numbers(number in any::<f64>().prop_filter("finite", |number| number.is_finite())) {
            let value = otlp_any(any_value::Value::DoubleValue(number));
            let expected = serde_json::to_string(&Value::Number(Number::from_f64(number).unwrap()))
                .unwrap();
            assert_eq!(canonical_any_value_json(&value).unwrap(), expected);
        }
    }
}
