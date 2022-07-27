use std::sync::Arc;

use my_no_sql_tcp_shared::{MyNoSqlReaderTcpSerializer, TcpContract};
use my_tcp_sockets::{tcp_connection::SocketConnection, ConnectionEvent, SocketEventCallback};

use crate::subscribers::Subscribers;

pub type TcpConnection = SocketConnection<TcpContract, MyNoSqlReaderTcpSerializer>;
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
        tcp_contract: TcpContract,
        _connection: Arc<TcpConnection>,
    ) {
        match tcp_contract {
            TcpContract::Ping => {}
            TcpContract::Pong => {}
            TcpContract::Greeting { name: _ } => {}
            TcpContract::Subscribe { table_name: _ } => {}
            TcpContract::InitTable { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().init_table(data).await;
                }
            }
            TcpContract::InitPartition {
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
            TcpContract::UpdateRows { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().update_rows(data).await;
                }
            }
            TcpContract::DeleteRows { table_name, rows } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()).await {
                    update_event.as_ref().delete_rows(rows).await;
                }
            }
            TcpContract::Error { message } => {
                panic!("Server error: {}", message);
            }
            TcpContract::GreetingFromNode {
                node_location: _,
                node_version: _,
            } => {}
            TcpContract::SubscribeAsNode(_) => {}
            TcpContract::Unsubscribe(_) => {}
            TcpContract::TableNotFound(_) => {}
        }
    }
}

#[async_trait::async_trait]
impl SocketEventCallback<TcpContract, MyNoSqlReaderTcpSerializer> for TcpEvents {
    async fn handle(
        &self,
        connection_event: ConnectionEvent<TcpContract, MyNoSqlReaderTcpSerializer>,
    ) {
        match connection_event {
            ConnectionEvent::Connected(connection) => {
                let contract = TcpContract::Greeting {
                    name: self.app_name.to_string(),
                };

                connection.send(contract).await;

                for table in self.subscribers.get_tables_to_subscirbe().await {
                    let contract = TcpContract::Subscribe {
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
