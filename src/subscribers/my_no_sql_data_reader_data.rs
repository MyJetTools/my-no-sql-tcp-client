use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::MyNoSqlEntity;

pub struct MyNoSqlDataReaderData<TMyNoSqlEntity: MyNoSqlEntity> {
    table_name: String,
    entities: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity> MyNoSqlDataReaderData<TMyNoSqlEntity> {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            entities: None,
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

    pub fn init_table(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        let entities = self.get_init_table();

        entities.clear();
        for (partition_key, src_entities_by_partition) in data {
            entities.insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = entities.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities_by_partition {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn init_partition(
        &mut self,
        partition_key: &str,
        src_entities: HashMap<String, Vec<TMyNoSqlEntity>>,
    ) {
        let entities = self.get_init_table();

        if let Some(partition) = entities.get_mut(partition_key) {
            partition.clear();
        } else {
            entities.insert(partition_key.to_string(), BTreeMap::new());
        }

        let by_partition = entities.get_mut(partition_key).unwrap();

        for (_, src_by_partition) in src_entities {
            for entity in src_by_partition {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn update_rows(&mut self, src_data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        let entities = self.get_init_table();

        for (partition_key, src_entities) in src_data {
            entities.insert(partition_key.to_string(), BTreeMap::new());

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
