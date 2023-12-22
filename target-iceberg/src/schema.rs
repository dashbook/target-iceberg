use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use singer::schema::{Compound, DateFormat, JsonSchema, Primitive, Type};

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
                Type::PrimitiveDate { r#type: _, format } => {
                    Field::new(trim_name(name), primitivedate_to_arrow(format), false)
                }
                Type::SingleDate { r#type: _, format } => {
                    Field::new(trim_name(name), primitivedate_to_arrow(format), false)
                }
                Type::VariantDate { r#type: _, format } => {
                    Field::new(trim_name(name), primitivedate_to_arrow(format), true)
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

#[inline]
pub fn primitivedate_to_arrow(input: &DateFormat) -> DataType {
    match input {
        DateFormat::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}
