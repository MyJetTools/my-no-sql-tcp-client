use std::{sync::Arc, time::Duration};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::MyNoSqlReaderTcpSerializer;
use my_tcp_sockets::TcpClient;
use rust_extensions::{ApplicationStates, Logger};
use serde::de::DeserializeOwned;

use crate::{
    subscribers::{MyNoSqlDataRaderCallBacks, MyNoSqlDataReader},
    tcp_events::TcpEvents,
};

pub struct MyNoSqlTcpConnection {
    tcp_client: TcpClient,
    pub ping_timeout: Duration,
    pub connect_timeout: Duration,
    pub tcp_events: Arc<TcpEvents>,
    pub app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    pub logger: Arc<dyn Logger + Send + Sync + 'static>,
}

impl MyNoSqlTcpConnection {
    pub fn new(
        host_port: String,
        app_name: String,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        Self {
            tcp_client: TcpClient::new("MyNoSqlClient".to_string(), host_port),
            ping_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
            tcp_events: Arc::new(TcpEvents::new(app_name)),
            app_states,
            logger,
        }
    }

    pub async fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
    >(
        &self,
        table_name: String,
    ) -> Arc<MyNoSqlDataReader<TMyNoSqlEntity>> {
        self.tcp_events
            .subscribers
            .create_subscriber::<TMyNoSqlEntity, ()>(
                table_name,
                None,
                self.app_states.clone(),
                self.logger.clone(),
            )
            .await
    }

    pub async fn get_reader_with_callbaclks<
        TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
        TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity> + Sync + Send + 'static,
    >(
        &self,
        table_name: String,
        callback: TMyNoSqlDataRaderCallBacks,
    ) -> Arc<MyNoSqlDataReader<TMyNoSqlEntity>> {
        self.tcp_events
            .subscribers
            .create_subscriber(
                table_name,
                Some(Arc::new(callback)),
                self.app_states.clone(),
                self.logger.clone(),
            )
            .await
    }

    pub async fn start(&self) {
        self.tcp_client
            .start(
                Arc::new(|| -> MyNoSqlReaderTcpSerializer { MyNoSqlReaderTcpSerializer::new() }),
                self.tcp_events.clone(),
                self.logger.clone(),
            )
            .await
    }
}
