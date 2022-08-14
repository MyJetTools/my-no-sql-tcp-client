use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use my_no_sql_server_abstractions::MyNoSqlEntity;
use rust_extensions::ApplicationStates;
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use super::{MyNoSqlDataRaderCallBacks, MyNoSqlDataReaderData, UpdateEvent};

pub struct MyNoSqlDataReader<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    data: RwLock<MyNoSqlDataReaderData<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReader<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
{
    pub async fn new(
        table_name: String,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    ) -> Self {
        Self {
            data: RwLock::new(MyNoSqlDataReaderData::new(table_name, app_states).await),
        }
    }

    pub async fn get_table_snapshot(
        &self,
    ) -> Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        let reader = self.data.read().await;

        return reader.get_table_snapshot();
    }

    pub async fn assign_callback<
        TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataRaderCallBacks>,
    ) {
        let mut write_access = self.data.write().await;
        write_access.assign_callback(callbacks).await;
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let reader = self.data.read().await;
        reader.get_by_partition(partition_key)
    }

    pub async fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let reader = self.data.read().await;
        reader.get_by_partition_as_vec(partition_key)
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let reader = self.data.read().await;
        reader.get_entity(partition_key, row_key)
    }

    pub fn deserialize<'s>(&self, data: &[u8]) -> TMyNoSqlEntity {
        let result = serde_json::from_slice(data).unwrap();
        result
    }

    pub fn deserialize_array(&self, data: &[u8]) -> HashMap<String, Vec<TMyNoSqlEntity>> {
        let elements: Vec<TMyNoSqlEntity> = serde_json::from_slice(data).unwrap();

        let mut result = HashMap::new();
        for el in elements {
            let partition_key = el.get_partition_key();
            if !result.contains_key(partition_key) {
                result.insert(partition_key.to_string(), Vec::new());
            }

            result.get_mut(partition_key).unwrap().push(el);
        }

        result
    }
}

#[async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned> UpdateEvent
    for MyNoSqlDataReader<TMyNoSqlEntity>
{
    async fn init_table(&self, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.data.write().await;
        write_access.init_table(data).await;
    }

    async fn init_partition(&self, partition_key: &str, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.data.write().await;
        write_access.init_partition(partition_key, data).await;
    }

    async fn update_rows(&self, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.data.write().await;
        write_access.update_rows(data);
    }

    async fn delete_rows(&self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        let mut write_access = self.data.write().await;
        write_access.delete_rows(rows_to_delete);
    }
}
