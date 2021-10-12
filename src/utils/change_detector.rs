use std::collections::BTreeMap;

use crate::MyNoSqlEntity;

pub struct ChangeDetector {}

pub enum Changes<TMyNoSqlEntity: MyNoSqlEntity> {
    Removed(TMyNoSqlEntity),
    Updated(TMyNoSqlEntity),
}

impl ChangeDetector {
    pub fn detect_change_partition<TMyNoSqlEntity: MyNoSqlEntity + Clone>(
        &mut self, 
        before: &mut BTreeMap<String, BTreeMap<String, TMyNoSqlEntity>>, 
        after: &mut BTreeMap<String, BTreeMap<String, TMyNoSqlEntity>>) -> Vec<Changes::<TMyNoSqlEntity>> {
        let mut changes: Vec<Changes::<TMyNoSqlEntity>> = vec![];
        for (partition_key, table)  in before.iter() {
            let after_table = &mut after.remove(partition_key);
            match after_table {
                Some(after_table_res) => {
                    let table_changes = self.detect_change_rows(table, after_table_res);
                    changes.extend(table_changes);
                }
                None => {
                    for (_, value) in table.iter() {
                        changes.push(Changes::Removed(value.clone()));
                    }
                }
            }
        }

        let after_keys: Vec<&String> = after.iter().map(|(key, _)| {key}).collect();
        if after_keys.len() != 0 {
            for row_key  in after_keys {
                let after_entity = after.get(row_key);
                match after_entity {
                    Some(res) => {
                        for (_, value) in res.iter() {
                            changes.push(Changes::Updated(value.clone()));
                        }
                    }
                    None => {
                    }
                }
            }
        }

        changes
    }

    pub fn detect_change_rows<TMyNoSqlEntity: MyNoSqlEntity + Clone>(&self, before: &BTreeMap<String, TMyNoSqlEntity>, after: &mut BTreeMap<String, TMyNoSqlEntity>) -> Vec<Changes::<TMyNoSqlEntity>> {
        let mut changes: Vec<Changes::<TMyNoSqlEntity>> = vec![];
        let before_keys: Vec<&String> = before.iter().map(|(key, _)| {key}).collect();
        for row_key  in before_keys {
            let before_entity = before.get(row_key).unwrap();
            let after_entity = after.remove(row_key);
            match after_entity {
                Some(res) => {
                    if res.get_time_stamp().unix_microseconds > before_entity.get_time_stamp().unix_microseconds {
                        changes.push(Changes::Updated(res));
                    }
                }
                None => {
                    changes.push(Changes::Removed(before_entity.clone()));
                }
            }
        }

        let after_keys: Vec<&String> = after.iter().map(|(key, _)| {key}).collect();
        if after_keys.len() != 0 {
            for row_key  in after_keys {
                let after_entity = after.get(row_key);
                match after_entity {
                    Some(res) => {
                            changes.push(Changes::Updated(res.clone()));
                    }
                    None => {
                        //Can't happen
                    }
                }
            }
        }

        changes
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap};

    use super::ChangeDetector;
    use crate::MyNoSqlEntity;
    use crate::subscribers::MyNoSqlDataReaderData;
    use crate::utils::change_detector::Changes;
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    #[derive(Debug, Clone)]
    struct SomeNoSql{
        len: i32,
        partition_key: String,
        row_key: String,
        time_stamp: DateTimeAsMicroseconds,
    }

    impl SomeNoSql {
        fn new(len: i32,
            partition_key: String,
            row_key: String,
            time_stamp: DateTimeAsMicroseconds,) -> Self {
            
                SomeNoSql {
                    len: len,
                    partition_key: partition_key,
                    row_key: row_key,
                    time_stamp: time_stamp
            }
        }
    }

    impl MyNoSqlEntity for SomeNoSql {
        fn get_partition_key(&self) -> &str {
            &self.partition_key[..]
        }
        fn get_row_key(&self) -> &str {
            &self.row_key[..]
        }
        fn get_time_stamp(&self) -> DateTimeAsMicroseconds {
            self.time_stamp
        }
    } 

    #[test]
    fn updated_entities() {
        let mut change_detector = ChangeDetector {};
        let mut data_reader: MyNoSqlDataReaderData<SomeNoSql> = MyNoSqlDataReaderData::<SomeNoSql>::new();
        let mut before_data = HashMap::<String, Vec<SomeNoSql>>::new();
        let now_before = DateTimeAsMicroseconds::now();
        before_data.insert("PartitionKey1".to_string(), 
        vec![SomeNoSql::new(1, "PartitionKey1".to_string(), "row_key_1".to_string(), now_before),
                SomeNoSql::new(2, "PartitionKey1".to_string(), "row_key_2".to_string(), now_before)]);

        data_reader.init_table(before_data);
        let before = &mut data_reader.readers;

        let mut data_reader: MyNoSqlDataReaderData<SomeNoSql> = MyNoSqlDataReaderData::<SomeNoSql>::new();
        let mut after_data =HashMap::<String, Vec<SomeNoSql>>::new();
        let now_after = DateTimeAsMicroseconds::now();
        after_data.insert("PartitionKey1".to_string(), 
        vec![SomeNoSql::new(1, "PartitionKey1".to_string(), "row_key_1".to_string(), now_after),
                SomeNoSql::new(2, "PartitionKey1".to_string(), "row_key_2".to_string(), now_after)]);
        after_data.insert("PartitionKey2".to_string(), 
                vec![SomeNoSql::new(3, "PartitionKey2".to_string(), "row_key_3".to_string(), now_after),
                        SomeNoSql::new(4, "PartitionKey2".to_string(), "row_key_4".to_string(), now_after)]);

        data_reader.init_table(after_data);
        let after = &mut data_reader.readers;

        let changes = change_detector.detect_change_partition(before, after);

        assert!(changes.len() == 4);

        for change in changes {
            match change {
                Changes::<SomeNoSql>::Updated(upd) => {
                    assert_eq!(upd.get_time_stamp().unix_microseconds, now_after.unix_microseconds);
                }
                Changes::<SomeNoSql>::Removed(_rmd) => {
                    assert!(false);
                }
            };
        }
    }

    #[test]
    fn removed_entities() {
        let mut change_detector = ChangeDetector {};
        let mut data_reader: MyNoSqlDataReaderData<SomeNoSql> = MyNoSqlDataReaderData::<SomeNoSql>::new();
        let mut before_data = HashMap::<String, Vec<SomeNoSql>>::new();
        let now_before = DateTimeAsMicroseconds::now();
        before_data.insert("PartitionKey1".to_string(), 
        vec![SomeNoSql::new(1, "PartitionKey1".to_string(), "row_key_1".to_string(), now_before),
                SomeNoSql::new(2, "PartitionKey1".to_string(), "row_key_2".to_string(), now_before)]);

        data_reader.init_table(before_data);
        let before = &mut data_reader.readers;

        let mut data_reader: MyNoSqlDataReaderData<SomeNoSql> = MyNoSqlDataReaderData::<SomeNoSql>::new();
        let mut after_data = HashMap::<String, Vec<SomeNoSql>>::new();
        after_data.insert("PartitionKey1".to_string(), 
        vec![]);

        data_reader.init_table(after_data);
        let after = &mut data_reader.readers;

        let changes = change_detector.detect_change_partition(before, after);

        assert!(changes.len() == 2);

        for change in changes {
            match change {
                Changes::<SomeNoSql>::Updated(_upd) => {
                    assert!(false);
                }
                Changes::<SomeNoSql>::Removed(rmd) => {
                    assert_eq!(rmd.get_time_stamp().unix_microseconds, now_before.unix_microseconds);
                }
            };
        }
    }
}