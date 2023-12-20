use std::{collections::HashMap, io::BufRead, sync::Arc};

use anyhow::anyhow;
use arrow::{
    datatypes::{DataType, Schema as ArrowSchema},
    error::ArrowError,
    json::ReaderBuilder,
};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
    stream, FutureExt, SinkExt, StreamExt, TryStreamExt,
};
use iceberg_rust::{
    arrow::write::write_parquet_partitioned,
    catalog::{bucket::parse_bucket, identifier::Identifier, tabular::Tabular},
    util::strip_prefix,
};
use singer::messages::Message;

use serde_json::Value as JsonValue;

use crate::{
    error::SingerIcebergError,
    plugin::TargetPlugin,
    schema::{convert_field, convert_schema, schema_to_arrow},
};

static ARROW_BATCH_SIZE: usize = 8192;

pub async fn ingest(
    plugin: Arc<dyn TargetPlugin>,
    input: &mut dyn BufRead,
) -> Result<(), SingerIcebergError> {
    let streams = plugin.streams();
    // Create sender and reviever for every stream
    let (senders, recievers): (
        HashMap<String, UnboundedSender<Message>>,
        Vec<UnboundedReceiver<Message>>,
    ) = streams
        .keys()
        .map(|stream| {
            let (s, r) = unbounded();
            ((stream.clone(), s), r)
        })
        .unzip();

    let (mut state_sender, state_reciever) = unbounded();

    let state = Arc::new(Mutex::new(
        state_reciever
            .fold(JsonValue::Null, |acc, x| async move {
                match x {
                    Message::State(state) => state.value,
                    _ => acc,
                }
            })
            .boxed_local(),
    ));

    // Process messages for every stream
    let handle = stream::iter(recievers.into_iter())
        .map(Ok::<_, SingerIcebergError>)
        .try_for_each_concurrent(None, |mut messages| {
            let plugin = plugin.clone();
            let streams = streams.clone();
            let state = state.clone();
            async move {
                let schema = match messages.next().await.ok_or(SingerIcebergError::Unknown)? {
                    Message::Schema(schema) => Ok(schema),
                    _ => Err(SingerIcebergError::NoSchema),
                }?;

                let stream = schema.stream;

                let identifier =
                    streams
                        .get(&stream)
                        .ok_or(SingerIcebergError::Anyhow(anyhow!(
                            "Stream {} not present in config",
                            &stream
                        )))?;

                let compiled_schema =
                    jsonschema::JSONSchema::compile(&serde_json::to_value(&schema.schema)?)
                        .unwrap();

                let arrow_schema = Arc::new(schema_to_arrow(&schema.schema)?);

                let catalog = plugin.catalog().await?;

                let table = catalog
                    .clone()
                    .load_table(&Identifier::parse(identifier)?)
                    .await?;

                let mut table = if let Tabular::Table(table) = table {
                    table
                } else {
                    return Err(SingerIcebergError::Unknown);
                };

                let table_schema = table
                    .metadata()
                    .current_schema(plugin.branch().as_deref())?;

                let table_arrow_schema: Arc<ArrowSchema> =
                    Arc::new((&table_schema.fields).try_into()?);

                let partition_spec = table.metadata().default_partition_spec()?;

                let conversions: Arc<HashMap<String, DataType>> = Arc::new(
                    arrow_schema
                        .all_fields()
                        .into_iter()
                        .filter_map(|arrow| {
                            let table = table_arrow_schema.field_with_name(arrow.name()).ok()?;
                            if arrow.data_type() == table.data_type() {
                                None
                            } else {
                                Some((arrow.name().clone(), table.data_type().clone()))
                            }
                        })
                        .collect(),
                );

                let intermediate_schema = Arc::new(convert_schema(&arrow_schema, &conversions)?);

                let batches = messages
                    .filter_map(|message| async move {
                        match message {
                            Message::Record(record) => Some(record),
                            _ => None,
                        }
                    })
                    // Check if record conforms to schema
                    .map(|message| {
                        if if let Err(_) = compiled_schema.validate(&message.record) {
                            false
                        } else {
                            true
                        } {
                            let mut value = message.record;
                            for (name, field) in conversions.as_ref() {
                                convert_field(&mut value, name, field)?;
                            }
                            Ok(value)
                        } else {
                            Err(SingerIcebergError::SchemaValidation)
                        }
                    })
                    .try_chunks(ARROW_BATCH_SIZE)
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                    // Convert messages to arrow batches
                    .and_then(|batches| {
                        let intermediate_schema = intermediate_schema.clone();
                        async move {
                            let mut decoder = ReaderBuilder::new(intermediate_schema.clone())
                                .build_decoder()
                                .unwrap();
                            decoder.serialize(&batches)?;
                            let record_batch = decoder.flush()?.ok_or(ArrowError::MemoryError(
                                "Data of recordbatch is empty.".to_string(),
                            ))?;
                            Ok(record_batch)
                        }
                    });

                let location: String = strip_prefix(&table.metadata().location);

                let bucket = parse_bucket(&table.metadata().location)?;

                let files = write_parquet_partitioned(
                    &location,
                    table_schema,
                    partition_spec,
                    batches,
                    catalog.object_store(bucket).clone(),
                )
                .await?;

                let state = match state.lock().await.as_mut().await {
                    JsonValue::Object(object) => Ok(object),
                    _ => Err(SingerIcebergError::Anyhow(anyhow!(
                        "State value has to be an object."
                    ))),
                }?;

                let stream_state = state.get(&stream).and_then(|x| match x {
                    JsonValue::String(s) => Some(s),
                    _ => None,
                });

                let transaction = table
                    .new_transaction(plugin.branch().as_deref())
                    .append(files);

                let transaction = match stream_state {
                    Some(x) => transaction
                        .update_properties(vec![("singer-bookmark".to_string(), x.to_string())]),
                    None => transaction,
                };

                transaction.commit().await?;

                Ok(())
            }
        });

    // Send messages to channel based on stream
    for line in input.lines() {
        let line = line.unwrap();

        if line.starts_with("{") {
            let message: Message = serde_json::from_str(&line).unwrap();
            match &message {
                Message::Schema(schema) => {
                    senders.get(&schema.stream).unwrap().send(message).await?
                }
                Message::Record(record) => {
                    senders.get(&record.stream).unwrap().send(message).await?
                }
                Message::State(_) => state_sender.send(message).await?,
                Message::ActivateVersion(_) => (),
            }
        }
    }

    senders.values().for_each(|sender| sender.close_channel());

    state_sender.close_channel();

    handle.await?;

    Ok(())
}
