use std::{collections::HashMap, sync::Arc};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;
use rust_extensions::ApplicationStates;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use super::{MyNoSqlDataReaderTcp, UpdateEvent};

pub struct Subscribers {
    subscribers: RwLock<HashMap<String, Arc<dyn UpdateEvent + Send + Sync + 'static>>>,
}

impl Subscribers {
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create_subscriber<TMyNoSqlEntity>(
        &self,

        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        sync_handler: Arc<SyncToMainNodeHandler>,
    ) -> Arc<MyNoSqlDataReaderTcp<TMyNoSqlEntity>>
    where
        TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
    {
        let mut write_access = self.subscribers.write().await;

        if write_access.contains_key(TMyNoSqlEntity::TABLE_NAME) {
            panic!(
                "You already subscribed for the table {}",
                TMyNoSqlEntity::TABLE_NAME
            );
        }

        let new_reader = MyNoSqlDataReaderTcp::new(app_states, sync_handler).await;

        let new_reader = Arc::new(new_reader);

        write_access.insert(TMyNoSqlEntity::TABLE_NAME.to_string(), new_reader.clone());

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

    pub async fn get_tables_to_subscribe(&self) -> Vec<String> {
        let read_access = self.subscribers.write().await;
        read_access.keys().map(|itm| itm.to_string()).collect()
    }
}
