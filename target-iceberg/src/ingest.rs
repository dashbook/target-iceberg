use std::{
    collections::HashMap,
    io::BufRead,
    ops::Deref,
    sync::{atomic::AtomicI64, Arc},
};

use anyhow::anyhow;
use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError, json::ReaderBuilder};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
    SinkExt, StreamExt, TryStreamExt,
};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::{identifier::Identifier, tabular::Tabular},
};
use singer::messages::Message;

use serde_json::Value as JsonValue;
use tracing::{debug, debug_span, Instrument};

use crate::{error::SingerIcebergError, plugin::TargetPlugin, state::SINGER_BOOKMARK};

static ARROW_BATCH_SIZE: usize = 8192;
static SINGER_VERSION: &str = "singer.version";

pub async fn ingest(
    plugin: Arc<dyn TargetPlugin>,
    input: &mut dyn BufRead,
) -> Result<(), SingerIcebergError> {
    let streams = plugin.streams();
    // Create sender and reviever for every stream
    let (mut message_senders, message_recievers): (
        HashMap<String, UnboundedSender<Message>>,
        Vec<UnboundedReceiver<Message>>,
    ) = streams
        .keys()
        .map(|stream| {
            let (s, r) = unbounded();
            ((stream.clone(), s), r)
        })
        .unzip();

    let (mut senders, recievers) = unbounded();

    for reciever in message_recievers {
        senders.send(reciever).await?;
    }

    let state = Arc::new(Mutex::new(JsonValue::Null));

    // Process messages for every stream
    let handle = recievers
        .map(Ok::<_, SingerIcebergError>)
        .try_for_each_concurrent(None, |mut messages| {
            let plugin = plugin.clone();
            let streams = streams;
            let state = state.clone();
            async move {
                let schema = match messages.next().await.ok_or(SingerIcebergError::Unknown)? {
                    Message::Schema(schema) => Ok(schema),
                    _ => Err(SingerIcebergError::NoSchema),
                }?;

                let active_version = Arc::new(AtomicI64::new(0));

                let stream = schema.stream;

                debug!("Syncing stream {}", &stream);
                debug!("Schema: {}", serde_json::to_string(&schema.schema)?);

                let identifier = &streams
                    .get(&stream)
                    .ok_or(SingerIcebergError::Anyhow(anyhow!(
                        "Stream {} not present in config",
                        &stream
                    )))?
                    .identifier;

                let compiled_schema =
                    jsonschema::JSONSchema::compile(&serde_json::to_value(&schema.schema)?)
                        .unwrap();

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

                let table = catalog.clone().load_tabular(&ident).await?;

                let mut table = if let Tabular::Table(table) = table {
                    table
                } else {
                    return Err(SingerIcebergError::Unknown);
                };

                let previous_version = table.metadata().properties.get(SINGER_VERSION);

                let table_schema = table
                    .metadata()
                    .current_schema(plugin.branch().as_deref())?;

                let table_arrow_schema: Arc<ArrowSchema> =
                    Arc::new((table_schema.fields()).try_into()?);

                let batches = messages
                    .filter_map(|message| {
                        let active_version = active_version.clone();
                        async move {
                            match message {
                                Message::Record(record) => Some(record),
                                Message::ActivateVersion(version) => {
                                    active_version.store(
                                        version.version,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    None
                                }
                                _ => None,
                            }
                        }
                    })
                    // Check if record conforms to schema
                    .map(|message| {
                        compiled_schema
                            .validate(&message.record)
                            .map_err(|mut err| {
                                let error = format!("{}", err.next().unwrap());
                                SingerIcebergError::Anyhow(anyhow::Error::msg(error))
                            })?;
                        let value = message.record;

                        Ok::<_, SingerIcebergError>(value)
                    })
                    .try_chunks(ARROW_BATCH_SIZE)
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                    // Convert messages to arrow batches
                    .and_then(|batches| {
                        let table_arrow_schema = table_arrow_schema.clone();
                        async move {
                            let mut decoder =
                                ReaderBuilder::new(table_arrow_schema.clone()).build_decoder()?;
                            decoder.serialize(&batches)?;
                            let record_batch = decoder.flush()?.ok_or(ArrowError::MemoryError(
                                "Data of recordbatch is empty.".to_string(),
                            ))?;
                            Ok(record_batch)
                        }
                    });

                let files = write_parquet_partitioned(
                    table.metadata(),
                    batches,
                    table.object_store(),
                    plugin.branch().as_deref(),
                )
                .await?;

                if !files.is_empty() {
                    let stream_state = {
                        let state = state.lock().await;
                        let state = match state.deref() {
                            JsonValue::Object(object) => match object.get("bookmarks") {
                                Some(JsonValue::Object(object)) => Ok(object),
                                _ => Err(SingerIcebergError::Anyhow(anyhow!(
                                    "State value has to be an object."
                                ))),
                            },
                            _ => Err(SingerIcebergError::Anyhow(anyhow!(
                                "State value has to be an object."
                            ))),
                        }?;

                        state.get(&stream).and_then(|x| match x {
                            JsonValue::Object(object) => Some(serde_json::to_string(&object).ok()?),
                            _ => None,
                        })
                    };

                    let active_version = active_version.load(std::sync::atomic::Ordering::Relaxed);

                    let transaction = if previous_version != Some(&active_version.to_string()) {
                        table
                            .new_transaction(plugin.branch().as_deref())
                            .rewrite(files)
                    } else {
                        table
                            .new_transaction(plugin.branch().as_deref())
                            .append(files)
                    };

                    if let Some(state) = &stream_state {
                        debug!("State of stream {}: {}", &stream, &state);
                    }

                    let transaction = match stream_state {
                        Some(x) => transaction
                            .update_properties(vec![(SINGER_BOOKMARK.to_string(), x.to_string())]),
                        None => transaction,
                    };

                    let transaction = if active_version != 0 {
                        transaction.update_properties(vec![(
                            SINGER_VERSION.to_string(),
                            active_version.to_string(),
                        )])
                    } else {
                        transaction
                    };

                    transaction.commit().await?;
                }

                Ok(())
            }
            .instrument(debug_span!("sync_stream"))
        });

    let mut versions: HashMap<String, i64> = HashMap::new();

    // Send messages to channel based on stream
    for line in input.lines() {
        let line = line?;

        if line.starts_with('{') {
            let message: Message = serde_json::from_str(&line)?;
            match &message {
                Message::Schema(schema) => {
                    message_senders
                        .get_mut(&schema.stream)
                        .ok_or(SingerIcebergError::Anyhow(anyhow!(
                            "Stream {} not found.",
                            &schema.stream,
                        )))?
                        .send(message)
                        .await?
                }
                Message::Record(record) => {
                    message_senders
                        .get_mut(&record.stream)
                        .ok_or(SingerIcebergError::Anyhow(anyhow!(
                            "Stream {} not found.",
                            &record.stream,
                        )))?
                        .send(message)
                        .await?
                }
                Message::ActivateVersion(record) => {
                    if let Some(version) = versions.get(&record.stream) {
                        if *version != record.version {
                            let (s, r) = unbounded();
                            message_senders
                                .insert(record.stream.clone(), s)
                                .ok_or(SingerIcebergError::Anyhow(anyhow!(
                                    "Stream must be available."
                                )))?
                                .close_channel();
                            senders.send(r).await?;
                            versions.insert(record.stream.clone(), record.version);
                        }
                    } else {
                        versions.insert(record.stream.clone(), record.version);
                    }
                    message_senders
                        .get_mut(&record.stream)
                        .ok_or(SingerIcebergError::Anyhow(anyhow!(
                            "Stream {} not found.",
                            &record.stream,
                        )))?
                        .send(message)
                        .await?
                }
                Message::State(new_state) => {
                    let mut state = state.lock().await;
                    *state = new_state.value.clone()
                }
            }
        }
    }

    senders.close_channel();

    message_senders
        .into_iter()
        .for_each(|(_, sender)| sender.close_channel());

    handle.await?;

    Ok(())
}
