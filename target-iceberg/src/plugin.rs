use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use iceberg_rust::catalog::Catalog;

use crate::error::SingerIcebergError;

#[async_trait]
pub trait TargetPlugin {
    async fn catalog(&self) -> Result<Arc<dyn Catalog>, SingerIcebergError>;
    fn bucket(&self) -> &str;
    fn streams(&self) -> Arc<HashMap<String, String>>;
    fn branch(&self) -> &Option<String>;
}
