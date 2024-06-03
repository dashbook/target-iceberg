use std::{fs, sync::Arc};

use anyhow::anyhow;
use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust::spec::partition::{PartitionField, PartitionSpec, Transform};
use iceberg_rust::table::Table;
use iceberg_rust::{catalog::identifier::Identifier, spec::schema::Schema};
use serde_json::{Map, Value};
use singer::catalog::{Catalog as SingerCatalog, Metadata, Stream as SingerStream};

use crate::{error::SingerIcebergError, plugin::TargetPlugin, schema::schema_to_arrow};

pub async fn select_streams(
    path: &str,
    plugin: Arc<dyn TargetPlugin>,
) -> Result<SingerCatalog, SingerIcebergError> {
    let json = fs::read_to_string(path)?;

    let streams = plugin.streams();

    let catalog: SingerCatalog = serde_json::from_str(&json)?;

    let streams = stream::iter(catalog.streams.into_iter())
        .filter_map(|stream| async move {
            let config = streams.get(&stream.tap_stream_id)?;
            Some((stream, config))
        })
        .then(|(mut stream, config)| {
            let plugin = plugin.clone();
            async move {
                if stream.metadata.is_some() {
                    if let Some(vec) = stream.metadata.as_mut() {
                        let opt = vec.iter_mut().find(|x| x.breadcrumb.is_empty());
                        if opt.is_some() {
                            if let Some(metadata) = opt {
                                if let Value::Object(metadata) = &mut metadata.metadata {
                                    metadata.insert("selected".to_string(), Value::Bool(true));
                                    metadata.insert(
                                        "replication-method".to_owned(),
                                        Value::String(config.replication_method.to_string()),
                                    );
                                }
                            }
                        } else {
                            vec.push(Metadata {
                                metadata: Value::Object(Map::from_iter(vec![
                                    ("selected".to_string(), Value::Bool(true)),
                                    (
                                        "replication-method".to_owned(),
                                        Value::String(config.replication_method.to_string()),
                                    ),
                                ])),
                                breadcrumb: vec![],
                            })
                        }
                    }
                } else {
                    stream.metadata = Some(vec![Metadata {
                        metadata: Value::Object(Map::from_iter(vec![
                            ("selected".to_string(), Value::Bool(true)),
                            (
                                "replication-method".to_owned(),
                                Value::String(config.replication_method.to_string()),
                            ),
                        ])),
                        breadcrumb: vec![],
                    }])
                };

                let ident = Identifier::try_new(
                    &config
                        .identifier
                        .split('.')
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .take(2)
                        .rev()
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>(),
                )?;

                let catalog = plugin.catalog().await?;

                if !catalog.tabular_exists(&ident).await? {
                    let arrow_schema = schema_to_arrow(&stream.schema)?;

                    let schema = Schema::builder()
                        .with_fields((&arrow_schema).try_into()?)
                        .build()
                        .map_err(iceberg_rust::spec::error::Error::from)?;

                    let base_path = plugin
                        .bucket()
                        .unwrap_or("")
                        .trim_end_matches("/")
                        .to_string()
                        + "/"
                        + &config.identifier.replace(".", "/");

                    let mut builder = Table::builder();
                    builder.with_name(ident.name());

                    if let Some(columns) = &config.partition_by {
                        builder.with_partition_spec(
                            PartitionSpec::builder()
                                .with_fields(
                                    columns
                                        .iter()
                                        .enumerate()
                                        .map(|(i, (column, transform))| {
                                            let field = schema.fields().get_name(column).ok_or(
                                                SingerIcebergError::Anyhow(anyhow!(
                                                    "Field {} doesn't exist in schema.",
                                                    column
                                                )),
                                            )?;
                                            let transform =
                                                serde_json::from_str::<Transform>(transform)?;
                                            Ok::<_, SingerIcebergError>(PartitionField::new(
                                                field.id,
                                                1000 + i as i32,
                                                column,
                                                transform,
                                            ))
                                        })
                                        .collect::<Result<_, _>>()?,
                                )
                                .build()
                                .map_err(iceberg_rust::spec::error::Error::from)?,
                        );
                    }

                    builder.with_location(&base_path).with_schema(schema);

                    builder.build(ident.namespace(), catalog).await?;
                }

                Ok::<_, SingerIcebergError>(stream)
            }
        })
        .try_collect::<Vec<SingerStream>>()
        .await?;

    Ok(SingerCatalog { streams })
}
