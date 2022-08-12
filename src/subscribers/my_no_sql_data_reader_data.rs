use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use my_no_sql_server_abstractions::MyNoSqlEntity;

use super::MyNoSqlDataRaderCallBacks;

pub struct MyNoSqlDataReaderData<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
> {
    table_name: String,
    entities: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
    callbacks: Option<Arc<TMyNoSqlDataRaderCallBacks>>,
}

impl<
        TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
        TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
    > MyNoSqlDataReaderData<TMyNoSqlEntity, TMyNoSqlDataRaderCallBacks>
{
    pub fn new(table_name: String, callbacks: Option<TMyNoSqlDataRaderCallBacks>) -> Self {
        Self {
            table_name,
            entities: None,
            callbacks: if let Some(callbacks) = callbacks {
                Some(Arc::new(callbacks))
            } else {
                None
            },
        }
    }

    fn get_init_table(&mut self) -> &mut BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        if self.entities.is_none() {
            println!("Initialized data for table {}", self.table_name);
            self.entities = Some(BTreeMap::new());
            return self.entities.as_mut().unwrap();
        }

        return self.entities.as_mut().unwrap();
    }

    pub async fn init_table(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        let mut new_table: BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> =
            BTreeMap::new();

        for (partition_key, src_entities_by_partition) in data {
            new_table.insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = new_table.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities_by_partition {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }

        let before = self.entities.replace(new_table);

        if let Some(callbacks) = self.callbacks.as_ref() {
            super::callback_triggers::trigger_table_difference(
                callbacks.as_ref(),
                before,
                self.entities.as_ref().unwrap(),
            )
            .await;
        }
    }

    pub async fn init_partition(
        &mut self,
        partition_key: &str,
        src_entities: HashMap<String, Vec<TMyNoSqlEntity>>,
    ) {
        let callbacks = self.callbacks.clone();

        let entities = self.get_init_table();

        let mut new_partition = BTreeMap::new();

        let before_partition = entities.remove(partition_key);

        for (row_key, entities) in src_entities {
            for entity in entities {
                new_partition.insert(row_key.clone(), Arc::new(entity));
            }
        }

        entities.insert(partition_key.to_string(), new_partition);

        if let Some(callbacks) = callbacks {
            super::callback_triggers::trigger_partition_difference(
                callbacks.as_ref(),
                partition_key,
                before_partition,
                entities.get(partition_key).unwrap(),
            )
            .await;
        }
    }

    pub fn update_rows(&mut self, src_data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        let entities = self.get_init_table();

        for (partition_key, src_entities) in src_data {
            if !entities.contains_key(partition_key.as_str()) {
                entities.insert(partition_key.to_string(), BTreeMap::new());
            }

            let by_partition = entities.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn delete_rows(&mut self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        let entities = self.get_init_table();

        for row in &rows_to_delete {
            let mut delete_partition = false;
            if let Some(partition) = entities.get_mut(row.partition_key.as_str()) {
                if partition.contains_key(row.row_key.as_str()) {
                    partition.remove(row.row_key.as_str());
                }

                delete_partition = partition.len() == 0;
            }

            if delete_partition {
                entities.remove(row.partition_key.as_str());
            }
        }
    }

    pub fn get_table_snapshot(
        &self,
    ) -> Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        let entities = self.entities.as_ref()?;

        return Some(entities.clone());
    }

    pub fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        let row = partition.get(row_key)?;

        Some(row.clone())
    }

    pub fn get_by_partition(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_ref()?;

        let partition = entities.get(partition_key)?;

        Some(partition.clone())
    }
}
