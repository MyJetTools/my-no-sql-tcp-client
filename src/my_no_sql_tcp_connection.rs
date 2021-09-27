use std::{sync::Arc, time::Duration};

use serde::de::DeserializeOwned;

use crate::{
    logger::{MyLogger, MyLoggerReader},
    subscribers::{MyNoSqlDataReader, Subscribers},
    MyNoSqlEntity,
};

pub struct MyNoSqlTcpConnection {
    logger: Option<MyLogger>,
    host_port: String,
    app_name: String,
    subscribers: Arc<Subscribers>,
    pub ping_timeout: Duration,
    pub connect_timeout: Duration,
}

impl MyNoSqlTcpConnection {
    pub fn new(host_port: String, app_name: String) -> Self {
        Self {
            host_port,
            app_name,
            subscribers: Arc::new(Subscribers::new()),
            logger: Some(MyLogger::new()),
            ping_timeout: Duration::from_secs(3),
            connect_timeout: Duration::from_secs(3),
        }
    }

    pub async fn get_reader<
        TMyNoSqlEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + 'static,
    >(
        &self,
        table_name: String,
    ) -> Arc<MyNoSqlDataReader<TMyNoSqlEntity>> {
        self.subscribers.create_subscriber(table_name).await
    }

    pub fn start(&mut self) {
        let mut logger = None;
        std::mem::swap(&mut logger, &mut self.logger);

        if logger.is_none() {
            panic!("Client is already started");
        }

        tokio::task::spawn(crate::tcp::new_connections::start(
            Arc::new(logger.unwrap()),
            self.host_port.clone(),
            self.app_name.clone(),
            self.ping_timeout,
            self.connect_timeout,
            self.subscribers.clone(),
        ));
    }

    pub fn get_logger_reader(&mut self) -> MyLoggerReader {
        if self.logger.is_none() {
            panic!("Logger reader can not be extracted because client is already started");
        }

        self.logger.as_mut().unwrap().get_reader()
    }
}
