use std::{collections::HashMap, fs, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use dashtool_common::ObjectStoreConfig;
use iceberg_catalog_sql::SqlCatalog;
use iceberg_rust::{catalog::Catalog, error::Error as IcebergError};
use object_store::{aws::AmazonS3Builder, memory::InMemory, ObjectStore};
use serde::{Deserialize, Serialize};
use target_iceberg::{
    error::SingerIcebergError,
    plugin::{BaseConfig, TargetPlugin},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(flatten)]
    pub base: BaseConfig,
    #[serde(flatten)]
    pub object_store: ObjectStoreConfig,
    pub catalog_name: String,
    pub catalog_url: String,
}

#[derive(Debug)]
pub(crate) struct SqlTargetPlugin {
    config: BaseConfig,
    catalog: Arc<dyn Catalog>,
}

impl SqlTargetPlugin {
    pub async fn new(path: &str) -> Result<Self, SingerIcebergError> {
        let config_json = fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&config_json)?;

        let object_store: Arc<dyn ObjectStore> = match &config.object_store {
            ObjectStoreConfig::Memory => Arc::new(InMemory::new()),
            ObjectStoreConfig::S3(s3_config) => Arc::new(
                AmazonS3Builder::new()
                    .with_region(&s3_config.aws_region)
                    .with_bucket_name(
                        config
                            .base
                            .bucket
                            .as_deref()
                            .ok_or(SingerIcebergError::Anyhow(anyhow!("No bucket specified.")))?,
                    )
                    .with_access_key_id(&s3_config.aws_access_key_id)
                    .with_secret_access_key(s3_config.aws_secret_access_key.as_ref().ok_or(
                        SingerIcebergError::Anyhow(anyhow!("No aws secret access key given.")),
                    )?)
                    .build()?,
            ),
        };

        let catalog = Arc::new(
            SqlCatalog::new(&config.catalog_url, &config.catalog_name, object_store)
                .await
                .map_err(IcebergError::from)?,
        );

        Ok(Self {
            config: config.base,
            catalog,
        })
    }
}

#[async_trait]
impl TargetPlugin for SqlTargetPlugin {
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
