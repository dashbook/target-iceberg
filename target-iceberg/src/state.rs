use std::{collections::HashMap, sync::Arc};

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
                let catalog = plugin.catalog().await?;

                let ident = Identifier::try_new(
                    &identifier
                        .split('.')
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .take(2)
                        .rev()
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>(),
                )?;

                let table = catalog.load_table(&ident).await?;

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
                .map(|(key, value)| (key, serde_json::from_str(&value).unwrap())),
        )),
    )]));

    Ok(state)
}
