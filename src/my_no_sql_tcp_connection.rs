use std::{sync::Arc, time::Duration};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::MyNoSqlReaderTcpSerializer;
use my_tcp_sockets::TcpClient;
use rust_extensions::{AppStates, Logger};
use serde::de::DeserializeOwned;

use crate::{subscribers::MyNoSqlDataReader, tcp_events::TcpEvents};

pub struct MyNoSqlTcpConnection {
    tcp_client: TcpClient,
    pub ping_timeout: Duration,
    pub connect_timeout: Duration,
    pub tcp_events: Arc<TcpEvents>,
    app_states: Arc<AppStates>,
    pub logger: Arc<dyn Logger + Send + Sync + 'static>,
}

impl MyNoSqlTcpConnection {
    pub fn new(
        host_port: String,
        app_name: String,
        logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        Self {
            tcp_client: TcpClient::new("MyNoSqlClient".to_string(), host_port),
            ping_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
            tcp_events: Arc::new(TcpEvents::new(app_name)),
            app_states: Arc::new(AppStates::create_un_initialized()),
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
            .create_subscriber(table_name, self.app_states.clone(), self.logger.clone())
            .await
    }

    pub async fn start(&self) {
        self.app_states.set_initialized();

        self.tcp_client
            .start(
                Arc::new(|| -> MyNoSqlReaderTcpSerializer { MyNoSqlReaderTcpSerializer::new() }),
                self.tcp_events.clone(),
                self.logger.clone(),
            )
            .await
    }
}
