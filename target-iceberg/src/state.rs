use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use futures::{lock::Mutex, stream, StreamExt, TryStreamExt};
use iceberg_rust::catalog::{identifier::Identifier, tabular::Tabular};
use serde_json::{Map, Value};

use crate::{error::SingerIcebergError, plugin::TargetPlugin};

pub async fn generate_state(plugin: Arc<dyn TargetPlugin>) -> Result<Value, SingerIcebergError> {
    let streams = plugin.streams();
    // let catalogs: Arc<Mutex<HashMap<String, Arc<dyn Catalog>>>> =
    //     Arc::new(Mutex::new(HashMap::new()));

    let bookmarks: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    stream::iter(streams.iter())
        .map(Ok::<_, SingerIcebergError>)
        .try_for_each_concurrent(None, |(stream, identifier)| {
            let bookmarks = bookmarks.clone();
            let plugin = plugin.clone();
            async move {
                let (table_namespace, table_name) = {
                    let mut parts: Vec<String> =
                        identifier.split(".").map(|x| x.to_owned()).collect();
                    let table_name = parts.pop().ok_or(SingerIcebergError::Anyhow(anyhow!(
                        "Table identifier doesn't contain table name."
                    )))?;
                    (parts.join("."), table_name)
                };
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

                let table = catalog.load_table(&Identifier::parse(identifier)?).await?;

                let table = if let Tabular::Table(table) = table {
                    table
                } else {
                    return Err(SingerIcebergError::Unknown);
                };

                if let Some(bookmark) = table.metadata().properties.get("singer-bookmark") {
                    bookmarks
                        .lock()
                        .await
                        .insert(stream.clone(), bookmark.clone());
                };
                Ok(())
            }
        })
        .await?;

    let state = Value::Object(Map::from_iter(vec![(
        "bookmarks".to_string(),
        Value::Object(Map::from_iter(
            Arc::try_unwrap(bookmarks)
                .unwrap()
                .into_inner()
                .into_iter()
                .map(|(key, value)| (key, Value::String(value))),
        )),
    )]));

    Ok(state)
}
