use std::{collections::HashMap, fs, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use dashbook_catalog::DashbookS3CatalogList;
use iceberg_rust::catalog::{Catalog, CatalogList};
use serde::{Deserialize, Serialize};
use target_iceberg::{
    error::SingerIcebergError,
    plugin::{BaseConfig, TargetPlugin},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(flatten)]
    pub base: BaseConfig,
    pub catalog: String,
    pub access_token: String,
    pub id_token: String,
}

#[derive(Debug)]
pub(crate) struct DashbookTargetPlugin {
    config: BaseConfig,
    catalog: Arc<dyn Catalog>,
}

impl DashbookTargetPlugin {
    pub async fn new(path: &str) -> Result<Self, SingerIcebergError> {
        let config_json = fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&config_json)?;

        let catalog_list = Arc::new(DashbookS3CatalogList::new(
            &config.access_token,
            &config.id_token,
        ));

        let catalog =
            catalog_list
                .catalog(&config.catalog)
                .await
                .ok_or(SingerIcebergError::Anyhow(anyhow!(
                    "Catalog {} not found.",
                    &config.catalog
                )))?;

        Ok(Self {
            config: config.base,
            catalog,
        })
    }
}

#[async_trait]
impl TargetPlugin for DashbookTargetPlugin {
    async fn catalog(&self) -> Result<Arc<dyn Catalog>, SingerIcebergError> {
        Ok(self.catalog.clone())
    }
    fn bucket(&self) -> Option<&str> {
        self.config.bucket.as_deref()
    }
    fn streams(&self) -> &HashMap<String, String> {
        &self.config.streams
    }
    fn branch(&self) -> &Option<String> {
        &self.config.branch
    }
}
