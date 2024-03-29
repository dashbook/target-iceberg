use std::{fs, sync::Arc};

use futures::{stream, StreamExt, TryStreamExt};
use iceberg_rust::{
    catalog::identifier::Identifier, spec::schema::Schema, table::table_builder::TableBuilder,
};
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

                if !catalog.table_exists(&ident).await? {
                    let arrow_schema = schema_to_arrow(&stream.schema)?;

                    let schema = Schema::builder()
                        .with_schema_id(1)
                        .with_fields((&arrow_schema).try_into()?)
                        .build()
                        .map_err(iceberg_rust_spec::error::Error::from)?;

                    let base_path = plugin
                        .bucket()
                        .unwrap_or("")
                        .trim_end_matches("/")
                        .to_string()
                        + "/"
                        + &config.identifier.replace(".", "/");

                    let mut builder = TableBuilder::new(ident, catalog)?;
                    builder
                        .location(&base_path)
                        .with_schema((1, schema))
                        .current_schema_id(1);

                    builder.build().await?;
                }

                Ok::<_, SingerIcebergError>(stream)
            }
        })
        .try_collect::<Vec<SingerStream>>()
        .await?;

    Ok(SingerCatalog { streams })
}
