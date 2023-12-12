use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use iceberg_rust::catalog::Catalog;
use serde::{Deserialize, Serialize};

use crate::error::SingerIcebergError;

#[async_trait]
pub trait TargetPlugin {
    async fn catalog(&self) -> Result<Arc<dyn Catalog>, SingerIcebergError>;
    fn bucket(&self) -> Option<&str>;
    fn streams(&self) -> &HashMap<String, String>;
    fn branch(&self) -> &Option<String>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseConfig {
    pub image: String,
    pub streams: HashMap<String, String>,
    pub bucket: Option<String>,
    pub branch: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(
    from = "Option<ObjectStoreConfigSerde>",
    into = "Option<ObjectStoreConfigSerde>"
)]
pub enum ObjectStoreConfig {
    FileSystem(FileSystemConfig),
    S3(S3Config),
    Memory,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileSystemConfig {
    pub path: String,
}

impl From<Option<ObjectStoreConfigSerde>> for ObjectStoreConfig {
    fn from(value: Option<ObjectStoreConfigSerde>) -> Self {
        match value {
            None => ObjectStoreConfig::Memory,
            Some(value) => match value {
                ObjectStoreConfigSerde::S3(value) => ObjectStoreConfig::S3(value),
                ObjectStoreConfigSerde::FileSystem(value) => ObjectStoreConfig::FileSystem(value),
            },
        }
    }
}

impl From<ObjectStoreConfig> for Option<ObjectStoreConfigSerde> {
    fn from(value: ObjectStoreConfig) -> Self {
        match value {
            ObjectStoreConfig::Memory => None,
            ObjectStoreConfig::S3(value) => Some(ObjectStoreConfigSerde::S3(value)),
            ObjectStoreConfig::FileSystem(value) => Some(ObjectStoreConfigSerde::FileSystem(value)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ObjectStoreConfigSerde {
    FileSystem(FileSystemConfig),
    S3(S3Config),
}

#[cfg(test)]
mod tests {

    use serde::{Deserialize, Serialize};

    use super::{BaseConfig, ObjectStoreConfig};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Config {
        #[serde(flatten)]
        pub base: BaseConfig,
        #[serde(flatten)]
        pub object_store: ObjectStoreConfig,
    }

    #[test]
    fn it_works() {
        let config: Config = serde_json::from_str(
            r#"
            {
                "image": "hello",
                "streams": {"hello": "world"}
            }
            "#,
        )
        .expect("Failed to parse config");

        let ObjectStoreConfig::Memory = config.object_store else {
            panic!("Wrong object_store type")
        };
    }
}
