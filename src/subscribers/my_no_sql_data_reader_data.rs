use std::collections::{BTreeMap, HashMap};

use crate::MyNoSqlEntity;

pub struct MyNoSqlDataReaderData<TMyNoSqlEntity: MyNoSqlEntity> {
    readers: BTreeMap<String, BTreeMap<String, TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity> MyNoSqlDataReaderData<TMyNoSqlEntity> {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn init_table(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        self.readers.clear();
        for (partition_key, entities) in data {
            self.readers
                .insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = self.readers.get_mut(partition_key.as_str()).unwrap();

            for entity in entities {
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn init_partition(
        &mut self,
        partition_key: &str,
        entities: HashMap<String, Vec<TMyNoSqlEntity>>,
    ) {
        if let Some(partition) = self.readers.get_mut(partition_key) {
            partition.clear();
        } else {
            self.readers
                .insert(partition_key.to_string(), BTreeMap::new());
        }

        let by_partition = self.readers.get_mut(partition_key).unwrap();

        for (_, src_by_partition) in entities {
            for entity in src_by_partition {
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn update_rows(&mut self, data: HashMap<String, Vec<TMyNoSqlEntity>>) {
        for (partition_key, entities) in data {
            self.readers
                .insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = self.readers.get_mut(partition_key.as_str()).unwrap();

            for entity in entities {
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }
    }

    pub fn delete_rows(&mut self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        for row in &rows_to_delete {
            let mut delete_partition = false;
            if let Some(partition) = self.readers.get_mut(row.partition_key.as_str()) {
                if partition.contains_key(row.row_key.as_str()) {
                    partition.remove(row.row_key.as_str());
                }

                delete_partition = partition.len() == 0;
            }

            if delete_partition {
                self.readers.remove(row.partition_key.as_str());
            }
        }
    }
}
