use std::sync::Arc;

use my_no_sql_tcp_shared::{MyNoSqlReaderTcpSerializer, MyNoSqlTcpContract};
use my_tcp_sockets::{tcp_connection::SocketConnection, ConnectionEvent, SocketEventCallback};

use crate::subscribers::Subscribers;

pub type TcpConnection = SocketConnection<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer>;
pub struct TcpEvents {
    app_name: String,
    pub subscribers: Subscribers,
}

impl TcpEvents {
    pub fn new(app_name: String) -> Self {
        Self {
            app_name,
            subscribers: Subscribers::new(),
        }
    }
    pub async fn handle_incoming_packet(
        &self,
        tcp_contract: MyNoSqlTcpContract,
        _connection: Arc<TcpConnection>,
    ) {
        match tcp_contract {
            MyNoSqlTcpContract::Ping => {}
            MyNoSqlTcpContract::Pong => {}
            MyNoSqlTcpContract::Greeting { name: _ } => {}
            MyNoSqlTcpContract::Subscribe { table_name: _ } => {}
            MyNoSqlTcpContract::InitTable { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().init_table(data).await;
                }
            }
            MyNoSqlTcpContract::InitPartition {
                table_name,
                partition_key,
                data,
            } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event
                        .as_ref()
                        .init_partition(partition_key.as_str(), data)
                        .await;
                }
            }
            MyNoSqlTcpContract::UpdateRows { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().update_rows(data).await;
                }
            }
            MyNoSqlTcpContract::DeleteRows { table_name, rows } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().delete_rows(rows).await;
                }
            }
            MyNoSqlTcpContract::Error { message } => {
                panic!("Server error: {}", message);
            }
            MyNoSqlTcpContract::GreetingFromNode {
                node_location: _,
                node_version: _,
                compress: _,
            } => {}
            MyNoSqlTcpContract::SubscribeAsNode(_) => {}
            MyNoSqlTcpContract::Unsubscribe(_) => {}
            MyNoSqlTcpContract::TableNotFound(_) => {}
            MyNoSqlTcpContract::CompressedPayload(_) => {}
        }
    }
}

#[async_trait::async_trait]
impl SocketEventCallback<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer> for TcpEvents {
    async fn handle(
        &self,
        connection_event: ConnectionEvent<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer>,
    ) {
        match connection_event {
            ConnectionEvent::Connected(connection) => {
                let contract = MyNoSqlTcpContract::Greeting {
                    name: self.app_name.to_string(),
                };

                connection.send(contract).await;

                for table in self.subscribers.get_tables_to_subscirbe().await {
                    let contract = MyNoSqlTcpContract::Subscribe {
                        table_name: table.to_string(),
                    };

                    connection.send(contract).await;
                }
            }
            ConnectionEvent::Disconnected(_connection) => {}
            ConnectionEvent::Payload {
                connection,
                payload,
            } => self.handle_incoming_packet(payload, connection).await,
        }
    }
}
