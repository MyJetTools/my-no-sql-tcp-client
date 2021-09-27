use std::{collections::HashMap, sync::Arc};

use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use crate::MyNoSqlEntity;

use super::{MyNoSqlDataReader, UpdateEvent};

pub struct Subscribers {
    subscribers: RwLock<HashMap<String, Arc<dyn UpdateEvent + Send + Sync + 'static>>>,
}

impl Subscribers {
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_subscriber<
        TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
    >(
        &self,
        table_name: String,
    ) -> Arc<MyNoSqlDataReader<TMyNoSqlEntity>> {
        let mut write_access = self.subscribers.write().await;

        if write_access.contains_key(table_name.as_str()) {
            panic!("You already subscribed for the table {}", table_name);
        }

        let new_reader = Arc::new(MyNoSqlDataReader::<TMyNoSqlEntity>::new());
        write_access.insert(table_name, new_reader.clone());

        new_reader
    }

    pub async fn get(
        &self,
        table_name: &str,
    ) -> Option<Arc<dyn UpdateEvent + Send + Sync + 'static>> {
        let read_access = self.subscribers.write().await;
        let result = read_access.get(table_name)?;
        Some(result.clone())
    }
}
