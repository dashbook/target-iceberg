use std::sync::Arc;

use clap::Parser;
use plugin::DashbookTargetPlugin;
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

    let plugin = Arc::new(DashbookTargetPlugin::new(&args.config)?);

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
        ingest(plugin.clone()).await
    }
}
