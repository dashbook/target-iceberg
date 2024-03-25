use std::{collections::HashMap, fmt::Display, sync::Arc};

use async_trait::async_trait;
use iceberg_rust::catalog::Catalog;
use serde::{Deserialize, Serialize};

use crate::error::SingerIcebergError;

#[async_trait]
pub trait TargetPlugin {
    async fn catalog(&self) -> Result<Arc<dyn Catalog>, SingerIcebergError>;
    fn bucket(&self) -> Option<&str>;
    fn streams(&self) -> &HashMap<String, StreamConfig>;
    fn branch(&self) -> &Option<String>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseConfig {
    pub image: String,
    pub streams: HashMap<String, StreamConfig>,
    pub bucket: Option<String>,
    pub branch: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamConfig {
    pub identifier: String,
    #[serde(default, rename = "replication-method")]
    pub replication_method: Replication,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub enum Replication {
    #[default]
    #[serde(rename = "FULL_TABLE")]
    FullTable,
    #[serde(rename = "INCREMENTAL")]
    Incremental,
    #[serde(rename = "LOG_BASED")]
    LogBased,
}

impl Display for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Replication::FullTable => write!("FULL_TABLE"),
            Replication::Incremental => write!("INCREMENTAL"),
            Replication::LogBased => write!("LOG_BASED"),
        }
    }
}

#[cfg(test)]
mod tests {

    use dashtool_common::ObjectStoreConfig;
    use serde::{Deserialize, Serialize};

    use super::BaseConfig;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Config {
        #[serde(flatten)]
        pub base: BaseConfig,
        #[serde(flatten)]
        pub object_store: ObjectStoreConfig,
    }

    #[test]
    fn test_config() {
        let config: Config = serde_json::from_str(
            r#"
            {
                "image": "hello",
                "streams": {"hello": { "identifier": "world", "replication": "LOG_BASED" }}
            }
            "#,
        )
        .expect("Failed to parse config");

        let ObjectStoreConfig::Memory = config.object_store else {
            panic!("Wrong object_store type")
        };
    }
}
