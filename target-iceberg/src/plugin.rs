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

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ObjectStoreConfig {
    Memory,
    FileSystem(FileSystemConfig),
    S3(S3Config),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileSystemConfig {
    pub path: String,
}
