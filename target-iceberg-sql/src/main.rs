use std::{
    io::{self, BufReader},
    sync::Arc,
};

use clap::Parser;
use plugin::SqlTargetPlugin;
use target_iceberg::{
    catalog::select_streams, error::SingerIcebergError, ingest::ingest, state::generate_state,
};

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
    let args = Args::parse();

    let plugin = Arc::new(SqlTargetPlugin::new(&args.config).await?);

    if args.state {
        let state = generate_state(plugin.clone()).await?;

        let json = serde_json::to_string(&state)?;

        print!("{}", &json);

        Ok(())
    } else if let Some(cat) = args.catalog {
        let catalog = select_streams(&cat, plugin.clone()).await?;

        let json = serde_json::to_string(&catalog)?;

        print!("{}", &json);

        Ok(())
    } else {
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
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_people() -> Result<(), Error> {
        let tempdir = tempdir()?;

        let config_path = tempdir.path().join("config.json");

        let mut config_file = File::create(config_path.clone())?;

        config_file.write_all(
            r#"
            {
            "image": "",
            "streams": {
                "people": "public.test.people"
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
            .load_table(&Identifier::parse("public.test.people")?)
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
            "image": "",
            "streams": {
                "inventory-orders": "public.inventory.orders"
            },
            "catalogUrl": "sqlite://",
            "catalogName": "public"
            }
        "#
            .as_bytes(),
        )?;

        let plugin = Arc::new(SqlTargetPlugin::new(config_path.as_path().to_str().unwrap()).await?);

        select_streams("../testdata/orders/catalog.json", plugin.clone()).await?;

        let input = File::open("../testdata/orders/input.txt")?;

        ingest(plugin.clone(), &mut BufReader::new(input)).await?;

        let catalog = plugin.catalog().await?;

        let table = if let Tabular::Table(table) = catalog
            .load_table(&Identifier::parse("public.inventory.orders")?)
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
}
