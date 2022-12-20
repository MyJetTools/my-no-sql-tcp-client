use std::{sync::Arc, time::Duration};

use my_no_sql_server_abstractions::MyNoSqlEntity;
use my_no_sql_tcp_shared::MyNoSqlReaderTcpSerializer;
use my_tcp_sockets::TcpClient;
use rust_extensions::AppStates;
use serde::de::DeserializeOwned;

use crate::{subscribers::MyNoSqlDataReader, tcp_events::TcpEvents, MyNoSqlTcpConnectionSettings};

pub struct TcpConnectionSettings {
    settings: Arc<dyn MyNoSqlTcpConnectionSettings + Sync + Send + 'static>,
}

#[async_trait::async_trait]
impl my_tcp_sockets::TcpClientSocketSettings for TcpConnectionSettings {
    async fn get_host_port(&self) -> String {
        self.settings.get_host_port().await
    }
}

pub struct MyNoSqlTcpConnection {
    tcp_client: TcpClient,
    pub ping_timeout: Duration,
    pub connect_timeout: Duration,
    pub tcp_events: Arc<TcpEvents>,
    app_states: Arc<AppStates>,
}

impl MyNoSqlTcpConnection {
    pub fn new(
        app_name: String,
        settings: Arc<dyn MyNoSqlTcpConnectionSettings + Sync + Send + 'static>,
    ) -> Self {
        let settings = TcpConnectionSettings { settings };

        Self {
            tcp_client: TcpClient::new("MyNoSqlClient".to_string(), Arc::new(settings)),
            ping_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
            tcp_events: Arc::new(TcpEvents::new(app_name)),
            app_states: Arc::new(AppStates::create_un_initialized()),
        }
    }

    pub async fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
    >(
        &self,
    ) -> Arc<MyNoSqlDataReader<TMyNoSqlEntity>> {
        self.tcp_events
            .subscribers
            .create_subscriber(self.app_states.clone())
            .await
    }

    pub async fn start(&self) {
        self.app_states.set_initialized();

        self.tcp_client
            .start(
                Arc::new(|| -> MyNoSqlReaderTcpSerializer { MyNoSqlReaderTcpSerializer::new() }),
                self.tcp_events.clone(),
                my_logger::LOGGER.clone(),
            )
            .await
    }
}
