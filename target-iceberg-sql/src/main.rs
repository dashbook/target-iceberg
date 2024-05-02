use std::{
    io::{self, BufReader},
    sync::Arc,
};

use clap::Parser;
use plugin::SqlTargetPlugin;
use target_iceberg::{
    catalog::select_streams, error::SingerIcebergError, ingest::ingest, state::generate_state,
};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

mod plugin;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, rename_all = "kebab-case")]
struct Args {
    /// Path to the config file
    #[arg(long, default_value = "target.json")]
    config: String,
    /// Path to the config file
    #[arg(long)]
    state: bool,
    /// Mark selected streams in catalog
    #[arg(long)]
    catalog: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), SingerIcebergError> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let plugin = Arc::new(SqlTargetPlugin::new(&args.config).await?);

    if args.state {
        info!("Generating state");

        let state = generate_state(plugin.clone()).await?;

        let json = serde_json::to_string(&state)?;

        debug!("{}", &json);

        print!("{}", &json);

        Ok(())
    } else if let Some(cat) = args.catalog {
        info!("Generating catalog");

        let catalog = select_streams(&cat, plugin.clone()).await?;

        let json = serde_json::to_string(&catalog)?;

        debug!("{}", &json);

        print!("{}", &json);

        Ok(())
    } else {
        info!("Start syncing ...");

        ingest(plugin.clone(), &mut BufReader::new(io::stdin())).await
    }
}

#[cfg(test)]
mod tests {
    use crate::SqlTargetPlugin;
    use anyhow::{anyhow, Error};
    use iceberg_rust::catalog::identifier::Identifier;
    use iceberg_rust::catalog::tabular::Tabular;
    use std::fs::File;
    use std::io::{BufReader, Write};
    use std::sync::Arc;
    use target_iceberg::catalog::select_streams;
    use target_iceberg::ingest::ingest;
    use target_iceberg::plugin::TargetPlugin;
    use target_iceberg::state::generate_state;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_people() -> Result<(), Error> {
        let tempdir = tempdir()?;

        let config_path = tempdir.path().join("config.json");

        let mut config_file = File::create(config_path.clone())?;

        config_file.write_all(
            r#"
            {
            "streams": {
                "people": { "identifier": "public.test.people" }
            },
            "catalogUrl": "sqlite://",
            "catalogName": "public"
            }
        "#
            .as_bytes(),
        )?;

        let plugin = Arc::new(SqlTargetPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        select_streams("../testdata/people/catalog.json", plugin.clone()).await?;

        let input = File::open("../testdata/people/input.txt")?;

        ingest(plugin.clone(), &mut BufReader::new(input)).await?;

        let catalog = plugin.catalog().await?;

        let table = if let Tabular::Table(table) = catalog
            .load_tabular(&Identifier::parse("test.people")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 100);

        Ok(())
    }
    #[tokio::test]
    async fn test_orders() -> Result<(), Error> {
        let tempdir = tempdir()?;

        let config_path = tempdir.path().join("config.json");

        let mut config_file = File::create(config_path.clone())?;

        config_file.write_all(
            r#"
            {
            "streams": {
                "inventory-orders": { "identifier": "public.inventory.orders" },
                "inventory-customers": { "identifier": "public.inventory.customers" },
                "inventory-products": { "identifier": "public.inventory.products" }
            },
            "catalogUrl": "sqlite://",
            "catalogName": "public"
            }
        "#
            .as_bytes(),
        )?;

        let plugin = Arc::new(SqlTargetPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        select_streams("../testdata/inventory/catalog.json", plugin.clone()).await?;

        let input = File::open("../testdata/inventory/input1.txt")?;

        ingest(plugin.clone(), &mut BufReader::new(input)).await?;

        let catalog = plugin.catalog().await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = orders_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 2);

        let orders_version = orders_table
            .metadata()
            .properties
            .get("singer-bookmark")
            .expect("Failed to get bookmark");

        assert_eq!(
            orders_version,
            r#"{"last_replication_method":"LOG_BASED","lsn":37125976,"version":1703756002202,"xmin":null}"#
        );

        let state = generate_state(plugin.clone()).await?;

        assert_eq!(
            state["bookmarks"]["inventory-orders"]["version"].to_string(),
            "1703756002202"
        );

        let products_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.products")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = products_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 5);

        let products_version = products_table
            .metadata()
            .properties
            .get("singer-bookmark")
            .expect("Failed to get bookmark");

        assert_eq!(
            products_version,
            r#"{"last_replication_method":"LOG_BASED","lsn":37125976,"version":1703756002235,"xmin":null}"#
        );

        let input = File::open("../testdata/inventory/input2.txt")?;

        ingest(plugin.clone(), &mut BufReader::new(input)).await?;

        let orders_table = if let Tabular::Table(table) = catalog
            .clone()
            .load_tabular(&Identifier::parse("inventory.orders")?)
            .await?
        {
            Ok(table)
        } else {
            Err(anyhow!("Not a table"))
        }?;

        let manifests = orders_table.manifests(None, None).await?;

        assert_eq!(manifests[0].added_rows_count.unwrap(), 4);

        Ok(())
    }
}
