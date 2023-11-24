use std::{fs, sync::Arc};

use anyhow::anyhow;
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

    // let catalogs: Arc<Mutex<HashMap<String, Arc<dyn Catalog>>>> =
    //     Arc::new(Mutex::new(HashMap::new()));

    // let catalog_name = config
    //     .catalog
    //     .split("/")
    //     .last()
    //     .ok_or(SingerIcebergError::Anyhow(anyhow!(
    //         "Catalog url doesn't contain catalog name."
    //     )))?;

    let streams = stream::iter(catalog.streams.into_iter())
        .filter_map(|stream| {
            let streams = streams.clone();
            async move {
                let idenfifier = streams.get(&stream.tap_stream_id).cloned()?;
                Some((stream, idenfifier))
            }
        })
        .then(|(mut stream, identifier)| {
            let plugin = plugin.clone();
            async move {
                if stream.metadata.is_some() {
                    if let Some(vec) = stream.metadata.as_mut() {
                        let opt = vec.iter_mut().find(|x| x.breadcrumb.is_empty());
                        if opt.is_some() {
                            if let Some(metadata) = opt {
                                if let Value::Object(metadata) = &mut metadata.metadata {
                                    metadata.insert("selected".to_string(), Value::Bool(true));
                                }
                            }
                        } else {
                            vec.push(Metadata {
                                metadata: Value::Object(Map::from_iter(vec![(
                                    "selected".to_string(),
                                    Value::Bool(true),
                                )])),
                                breadcrumb: vec![],
                            })
                        }
                    }
                } else {
                    stream.metadata = Some(vec![Metadata {
                        metadata: Value::Object(Map::from_iter(vec![(
                            "selected".to_string(),
                            Value::Bool(true),
                        )])),
                        breadcrumb: vec![],
                    }])
                };

                let (table_namespace, table_name) = {
                    let mut parts: Vec<String> =
                        identifier.split(".").map(|x| x.to_owned()).collect();
                    let table_name = parts.pop().ok_or(SingerIcebergError::Anyhow(anyhow!(
                        "Table identifier doesn't contain table name."
                    )))?;
                    (parts.join("."), table_name)
                };

                let ident = Identifier::parse(&identifier)?;

                let catalog = plugin.catalog(&table_namespace, &table_name).await?;

                // let role = get_role(
                //     &config.access_token,
                //     catalog_name,
                //     &table_namespace,
                //     &table_name,
                //     "read",
                // )
                // .await?;

                // let catalog = {
                //     let mut catalogs = catalogs.lock().await;
                //     match catalogs.get(&role) {
                //         Some(catalog) => catalog.clone(),
                //         None => {
                //             let catalog = get_catalog(
                //                 &config.catalog,
                //                 &config.access_token,
                //                 &config.id_token,
                //                 &table_namespace,
                //                 &table_name,
                //                 &role,
                //             )
                //             .await?;
                //             catalogs.insert(role, catalog.clone());
                //             catalog
                //         }
                //     }
                // };

                if !catalog.table_exists(&ident).await? {
                    let arrow_schema = schema_to_arrow(&stream.schema)?;

                    let schema = Schema {
                        schema_id: 1,
                        identifier_field_ids: None,
                        fields: (&arrow_schema).try_into()?,
                    };

                    let base_path = plugin.bucket().trim_end_matches("/").to_string()
                        + &identifier.replace(".", "/");

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
