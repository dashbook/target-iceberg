use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use chrono::{DateTime, NaiveDate};
use serde_json::Value;
use singer::schema::{Compound, JsonSchema, Primitive, Type};

use crate::error::SingerIcebergError;

pub fn schema_to_arrow(schema: &JsonSchema) -> Result<Schema, SingerIcebergError> {
    if let Type::Compound(Compound::Object(object)) = &schema.r#type {
        let mut builder = SchemaBuilder::new();
        for (name, r#type) in &object.properties {
            let field = match r#type {
                Type::Primitive { r#type } => {
                    Field::new(trim_name(name), primitive_to_arrow(r#type), false)
                }
                Type::Single { r#type } => {
                    Field::new(trim_name(name), primitive_to_arrow(&r#type[0]), false)
                }
                Type::Variant { r#type } => {
                    let value = match (&r#type[0], &r#type[1]) {
                        (prim, Primitive::Null) => prim,
                        (Primitive::Null, prim) => prim,
                        (first, _) => first,
                    };
                    Field::new(trim_name(name), primitive_to_arrow(value), true)
                }
                Type::Compound(_) => Field::new(trim_name(name), DataType::Null, true),
                Type::Empty(_) => Field::new(trim_name(name), DataType::Null, true),
            };
            builder.push(field);
        }
        Ok(builder.finish())
    } else {
        Err(SingerIcebergError::NoSchema)
    }
}

fn trim_name(name: &str) -> &str {
    name.trim().trim_start_matches("\"").trim_end_matches("\"")
}

#[inline]
pub fn primitive_to_arrow(input: &Primitive) -> DataType {
    match input {
        Primitive::Null => DataType::Null,
        Primitive::Boolean => DataType::Boolean,
        Primitive::Integer => DataType::Int32,
        Primitive::Number => DataType::Float32,
        Primitive::String => DataType::Utf8,
    }
}

pub(crate) fn convert_field(
    object: &mut Value,
    name: &str,
    data_type: &DataType,
) -> Result<(), SingerIcebergError> {
    if let Value::Object(object) = object {
        let field = object
            .get_mut(name)
            .ok_or(SingerIcebergError::SchemaValidation)?;
        match (data_type, &field) {
            (DataType::Date32, Value::String(value)) => {
                let days = DateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%z")?
                    .naive_utc()
                    .date()
                    .signed_duration_since(
                        // This is always the same and shouldn't fail
                        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                    )
                    .num_days() as i32;
                *field = Value::Number(days.into())
            }
            (DataType::Timestamp(TimeUnit::Microsecond, None), Value::String(value)) => {
                let days = DateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%z")?
                    .naive_utc()
                    .date()
                    .signed_duration_since(
                        // This is always the same and shouldn't fail
                        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                    )
                    .num_microseconds()
                    .ok_or(SingerIcebergError::SchemaValidation)?;
                *field = Value::Number(days.into())
            }
            _ => return Err(SingerIcebergError::SchemaValidation),
        };
        Ok(())
    } else {
        Err(SingerIcebergError::SchemaValidation)
    }
}

pub(crate) fn convert_schema(
    schema: &Schema,
    conversions: &HashMap<String, DataType>,
) -> Result<Schema, SingerIcebergError> {
    Ok(Schema::new(
        schema
            .fields()
            .into_iter()
            .map(|x| {
                if let Some(field) = conversions.get(x.name()) {
                    let data_type = match (x.data_type(), field) {
                        (DataType::Utf8, DataType::Date32) => DataType::Int32,
                        (DataType::Utf8, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
                            DataType::Int64
                        }
                        _ => return Err(SingerIcebergError::SchemaValidation),
                    };
                    Ok(Arc::new(Field::new(x.name(), data_type, x.is_nullable())))
                } else {
                    Ok(x.clone())
                }
            })
            .collect::<Result<Vec<_>, SingerIcebergError>>()?,
    ))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use serde_json::{Map, Value};

    use super::convert_field;

    #[test]
    fn test_convert_date() {
        let mut input = Value::Object(Map::from_iter(vec![(
            "test".to_string(),
            Value::String("2016-02-21T00:00:00+00:00".to_string()),
        )]));

        convert_field(&mut input, "test", &DataType::Date32).unwrap();

        if let Value::Object(object) = input {
            assert_eq!(object.get("test").unwrap().as_i64(), Some(16852));
        } else {
            panic!("Result must be an object.")
        }
    }
}
