use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use rust_extensions::lazy::LazyVec;

use super::MyNoSqlDataRaderCallBacks;

pub async fn trigger_table_difference<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataRaderCallBacks,
    before: Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>>,
    now_entities: &BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
) {
    match before {
        Some(before) => {
            trigger_old_and_new_table_difference(callbacks, before, now_entities).await;
        }
        None => {
            trigger_brand_new_table(callbacks, now_entities).await;
        }
    }
}

pub async fn trigger_brand_new_table<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataRaderCallBacks,
    now_entities: &BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
) {
    for (partition_key, now_partition) in now_entities {
        let mut added = LazyVec::new();
        for entity in now_partition.values() {
            added.add(entity.clone());
        }

        if let Some(added_entities) = added.get_result() {
            callbacks.added(partition_key, added_entities).await;
        }
    }
}

pub async fn trigger_old_and_new_table_difference<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataRaderCallBacks,
    mut before: BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
    now_entities: &BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
) {
    for (now_partition_key, now_partition) in now_entities {
        let before_partition = before.remove(now_partition_key);

        trigger_partition_difference(
            callbacks,
            now_partition_key,
            before_partition,
            now_partition,
        )
        .await;
    }
}

pub async fn trigger_partition_difference<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataRaderCallBacks,
    partition_key: &str,
    before_partition: Option<BTreeMap<String, Arc<TMyNoSqlEntity>>>,
    now_partition: &BTreeMap<String, Arc<TMyNoSqlEntity>>,
) {
    match before_partition {
        Some(mut before_partition) => {
            for (now_row_key, now_row) in now_partition {
                let mut updated = LazyVec::new();
                let mut added = LazyVec::new();

                match before_partition.remove(now_row_key) {
                    Some(_) => {
                        updated.add(now_row.clone());
                    }
                    None => {
                        added.add(now_row.clone());
                    }
                }

                if let Some(updated) = updated.get_result() {
                    callbacks.updated(partition_key, updated).await;
                }
                if let Some(added) = added.get_result() {
                    callbacks.added(partition_key, added).await;
                }
            }

            let mut deleted_entities = LazyVec::new();

            for (_, before_row) in before_partition {
                deleted_entities.add(before_row);
            }

            if let Some(deleted_entities) = deleted_entities.get_result() {
                callbacks.deleted(partition_key, deleted_entities).await;
            }
        }
        None => {
            trigger_brand_new_partition(callbacks, partition_key, now_partition).await;
        }
    }
}

pub async fn trigger_brand_new_partition<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataRaderCallBacks,
    partition_key: &str,
    partition: &BTreeMap<String, Arc<TMyNoSqlEntity>>,
) {
    let mut added = LazyVec::new();
    for entity in partition.values() {
        added.add(entity.clone());
    }

    if let Some(added_entities) = added.get_result() {
        callbacks.added(partition_key, added_entities).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    use my_no_sql_server_abstractions::MyNoSqlEntity;
    use tokio::sync::Mutex;

    use crate::subscribers::MyNoSqlDataRaderCallBacks;

    struct TestCallbacksInner {
        added: HashMap<String, Vec<Arc<TestRow>>>,
        updated: HashMap<String, Vec<Arc<TestRow>>>,
        deleted: HashMap<String, Vec<Arc<TestRow>>>,
    }

    pub struct TestCallbacks {
        data: Mutex<TestCallbacksInner>,
    }

    #[async_trait::async_trait]
    impl MyNoSqlDataRaderCallBacks<TestRow> for TestCallbacks {
        async fn added(&self, partition_key: &str, entities: Vec<Arc<TestRow>>) {
            let mut write_access = self.data.lock().await;
            match write_access.added.get_mut(partition_key) {
                Some(db_partition) => {
                    db_partition.extend(entities);
                }

                None => {
                    write_access
                        .added
                        .insert(partition_key.to_string(), entities);
                }
            }
        }

        async fn updated(&self, partition_key: &str, entities: Vec<Arc<TestRow>>) {
            let mut write_access = self.data.lock().await;
            match write_access.updated.get_mut(partition_key) {
                Some(db_partition) => {
                    db_partition.extend(entities);
                }

                None => {
                    write_access
                        .updated
                        .insert(partition_key.to_string(), entities);
                }
            }
        }

        async fn deleted(&self, partition_key: &str, entities: Vec<Arc<TestRow>>) {
            let mut write_access = self.data.lock().await;
            match write_access.deleted.get_mut(partition_key) {
                Some(db_partition) => {
                    db_partition.extend(entities);
                }

                None => {
                    write_access
                        .deleted
                        .insert(partition_key.to_string(), entities);
                }
            }
        }
    }
    pub struct TestRow {
        partition_key: String,
        row_key: String,
        timestamp: i64,
    }

    impl TestRow {
        pub fn new(partition_key: String, row_key: String, timestamp: i64) -> Self {
            TestRow {
                partition_key,
                row_key,
                timestamp,
            }
        }
    }

    impl MyNoSqlEntity for TestRow {
        fn get_partition_key(&self) -> &str {
            self.partition_key.as_str()
        }
        fn get_row_key(&self) -> &str {
            self.row_key.as_str()
        }
        fn get_time_stamp(&self) -> i64 {
            self.timestamp
        }
    }

    #[tokio::test]
    pub async fn test_brand_new_table() {
        let mut before = BTreeMap::new();

        let mut partition_rows = BTreeMap::new();

        partition_rows.insert(
            "PK1",
            Arc::new(TestRow::new("PK1".to_string(), "RK1".to_string(), 1)),
        );
        partition_rows.insert(
            "PK1",
            Arc::new(TestRow::new("PK1".to_string(), "RK2".to_string(), 1)),
        );

        before.insert("PK1".to_string(), partition_rows);
    }
}
