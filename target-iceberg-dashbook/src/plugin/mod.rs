use std::{collections::HashMap, fs, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use dashbook_catalog::{get_catalog, get_role};
use futures::lock::Mutex;
use iceberg_rust::catalog::Catalog;
use serde::{Deserialize, Serialize};
use target_iceberg::{error::SingerIcebergError, plugin::TargetPlugin};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub image: String,
    pub streams: HashMap<String, String>,
    pub catalog: String,
    pub bucket: String,
    pub access_token: String,
    pub id_token: String,
    pub branch: Option<String>,
}

#[derive(Debug)]
pub(crate) struct DashbookTargetPlugin {
    config: Config,
    catalogs: Arc<Mutex<HashMap<String, Arc<dyn Catalog>>>>,
}

impl DashbookTargetPlugin {
    pub fn new(path: &str) -> Result<Self, SingerIcebergError> {
        let config_json = fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&config_json)?;

        let catalogs: Arc<Mutex<HashMap<String, Arc<dyn Catalog>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        Ok(Self { config, catalogs })
    }
}

#[async_trait]
impl TargetPlugin for DashbookTargetPlugin {
    async fn catalog(
        &self,
        table_namespace: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Catalog>, SingerIcebergError> {
        let catalog_name =
            self.config
                .catalog
                .split("/")
                .last()
                .ok_or(SingerIcebergError::Anyhow(anyhow!(
                    "Catalog url doesn't contain catalog name."
                )))?;

        let role = get_role(
            &self.config.access_token,
            &catalog_name,
            table_namespace,
            table_name,
            "write",
        )
        .await
        .map_err(|err| SingerIcebergError::Anyhow(anyhow::Error::from(err)))?;

        let catalog = {
            let mut catalogs = self.catalogs.lock().await;
            match catalogs.get(&role) {
                Some(catalog) => catalog.clone(),
                None => {
                    let catalog = get_catalog(
                        &self.config.catalog,
                        &self.config.access_token,
                        &self.config.id_token,
                        &table_namespace,
                        &table_name,
                        &role,
                    )
                    .await
                    .map_err(|err| SingerIcebergError::Anyhow(anyhow::Error::from(err)))?;
                    catalogs.insert(role, catalog.clone());
                    catalog
                }
            }
        };
        Ok(catalog)
    }
    fn bucket(&self) -> &str {
        unimplemented!()
    }
    fn streams(&self) -> Arc<HashMap<String, String>> {
        unimplemented!()
    }
    fn branch(&self) -> &Option<String> {
        unimplemented!()
    }
}
