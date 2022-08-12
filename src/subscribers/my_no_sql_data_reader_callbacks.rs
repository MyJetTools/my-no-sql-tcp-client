use std::sync::Arc;

use my_no_sql_server_abstractions::MyNoSqlEntity;

#[async_trait::async_trait]
pub trait MyNoSqlDataRaderCallBacks<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    async fn added(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>);
    async fn updated(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>);
    async fn deleted(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>);
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static>
    MyNoSqlDataRaderCallBacks<TMyNoSqlEntity> for ()
{
    async fn added(&self, _partition_key: &str, _entity: Vec<Arc<TMyNoSqlEntity>>) {
        panic!("This is a dumb implementation")
    }

    async fn updated(&self, _partition_key: &str, _entity: Vec<Arc<TMyNoSqlEntity>>) {
        panic!("This is a dumb implementation")
    }

    async fn deleted(&self, _partition_key: &str, _entity: Vec<Arc<TMyNoSqlEntity>>) {
        panic!("This is a dumb implementation")
    }
}
