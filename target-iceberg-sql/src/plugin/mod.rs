use std::{collections::HashMap, fs, sync::Arc};

use async_trait::async_trait;
use iceberg_catalog_sql::SqlCatalog;
use iceberg_rust::{catalog::Catalog, error::Error as IcebergError};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use serde::{Deserialize, Serialize};
use target_iceberg::{error::SingerIcebergError, plugin::TargetPlugin};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub image: String,
    pub streams: HashMap<String, String>,
    pub name: String,
    pub url: String,
    pub region: String,
    pub bucket: String,
    pub access_token: String,
    pub id_token: String,
    pub branch: Option<String>,
}

#[derive(Debug)]
pub(crate) struct SqlTargetPlugin {
    config: Config,
    catalog: Arc<dyn Catalog>,
}

impl SqlTargetPlugin {
    pub async fn new(path: &str) -> Result<Self, SingerIcebergError> {
        let config_json = fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&config_json)?;

        let object_store = Arc::new(
            AmazonS3Builder::from_env()
                .with_region(&config.region)
                .with_bucket_name(&config.bucket)
                .build()?,
        ) as Arc<dyn ObjectStore>;

        let catalog = Arc::new(
            SqlCatalog::new(&config.url, &config.name, object_store)
                .await
                .map_err(IcebergError::from)?,
        );

        Ok(Self { config, catalog })
    }
}

#[async_trait]
impl TargetPlugin for SqlTargetPlugin {
    async fn catalog(
        &self,
        _table_namespace: &str,
        _table_name: &str,
    ) -> Result<Arc<dyn Catalog>, SingerIcebergError> {
        Ok(self.catalog.clone())
    }
    fn bucket(&self) -> &str {
        &self.config.bucket
    }
    fn streams(&self) -> Arc<HashMap<String, String>> {
        unimplemented!()
    }
    fn branch(&self) -> &Option<String> {
        unimplemented!()
    }
}
