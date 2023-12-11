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
