use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::MyNoSqlEntity;

pub struct MyNoSqlDataReaderData<TMyNoSqlEntity: MyNoSqlEntity> {
    entities: BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity> MyNoSqlDataReaderData<TMyNoSqlEntity> {
    pub fn new() -> Self {
        Self {
            entities: BTreeMap::new(),
        }
    }

    pub fn init_table(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        self.entities.clear();
        for (partition_key, entities) in data {
            self.entities
                .insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = self.entities.get_mut(partition_key.as_str()).unwrap();

            for entity in entities {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn init_partition(
        &mut self,
        partition_key: &str,
        entities: HashMap<String, Vec<TMyNoSqlEntity>>,
    ) {
        if let Some(partition) = self.entities.get_mut(partition_key) {
            partition.clear();
        } else {
            self.entities
                .insert(partition_key.to_string(), BTreeMap::new());
        }

        let by_partition = self.entities.get_mut(partition_key).unwrap();

        for (_, src_by_partition) in entities {
            for entity in src_by_partition {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn update_rows(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        for (partition_key, entities) in data {
            self.entities
                .insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = self.entities.get_mut(partition_key.as_str()).unwrap();

            for entity in entities {
                let entity = Arc::new(entity);
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn delete_rows(&mut self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        for row in &rows_to_delete {
            let mut delete_partition = false;
            if let Some(partition) = self.entities.get_mut(row.partition_key.as_str()) {
                if partition.contains_key(row.row_key.as_str()) {
                    partition.remove(row.row_key.as_str());
                }

                delete_partition = partition.len() == 0;
            }

            if delete_partition {
                self.entities.remove(row.partition_key.as_str());
            }
        }
    }

    pub fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>> {
        let partition = self.entities.get(partition_key)?;

        let row = partition.get(row_key)?;

        Some(row.clone())
    }

    pub fn get_by_partition(&self, partition_key: &str) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let partition = self.entities.get(partition_key)?;

        let mut result = Vec::new();

        for item in partition.values() {
            result.push(item.clone());
        }

        Some(result)
    }
}
