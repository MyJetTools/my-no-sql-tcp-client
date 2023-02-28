use std::sync::Arc;

use my_no_sql_server_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::sync_to_main::UpdateEntityStatisticsData;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::my_no_sql_data_reader::MyNoSqlDataReaderInner;

pub struct GetEntityBuilder<'s, TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static> {
    partition_key: &'s str,
    row_key: &'s str,
    update_statistic_data: UpdateEntityStatisticsData,
    inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
}

impl<'s, TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + 'static>
    GetEntityBuilder<'s, TMyNoSqlEntity>
{
    pub fn new(
        partition_key: &'s str,
        row_key: &'s str,
        inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
    ) -> Self {
        Self {
            partition_key,
            row_key,
            update_statistic_data: UpdateEntityStatisticsData::default(),
            inner,
        }
    }

    pub fn set_partition_last_read_moment(mut self) -> Self {
        self.update_statistic_data.partition_last_read_moment = true;
        self
    }

    pub fn set_row_last_read_moment(mut self) -> Self {
        self.update_statistic_data.row_last_read_moment = true;
        self
    }

    pub fn set_partition_expiration_moment(
        mut self,
        value: Option<DateTimeAsMicroseconds>,
    ) -> Self {
        self.update_statistic_data.partition_expiration_moment = Some(value);
        self
    }

    pub fn set_row_expiration_moment(mut self, value: Option<DateTimeAsMicroseconds>) -> Self {
        self.update_statistic_data.row_expiration_moment = Some(value);
        self
    }

    pub async fn execute(&self) -> Option<Arc<TMyNoSqlEntity>> {
        let result = {
            let reader = self.inner.get_data().read().await;
            reader.get_entity(self.partition_key, self.row_key)
        };

        if result.is_some() {
            self.inner
                .get_sync_handler()
                .event_notifier
                .update(
                    TMyNoSqlEntity::TABLE_NAME,
                    self.partition_key,
                    || [self.row_key].into_iter(),
                    &self.update_statistic_data,
                )
                .await;
        }

        result
    }
}
