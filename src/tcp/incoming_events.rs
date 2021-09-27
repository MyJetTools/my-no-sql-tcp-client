use std::sync::Arc;

use my_no_sql_tcp_shared::TcpContract;

use crate::subscribers::Subscribers;

use super::SocketConnection;

pub async fn connected(connection: Arc<SocketConnection>) {
    let connection_id = connection.id;
    let connected_result = tokio::task::spawn(handle_connected(connection)).await;

    if let Err(err) = connected_result {
        println!(
            "Panic at handeling tcp connected event for connection {}. Error: {:?}",
            connection_id, err
        );
    }
}

async fn handle_connected(connection: Arc<SocketConnection>) {
    //TODO - Remove if not needed
    //publishers.new_connection(connection.clone()).await;
    //subscribers.new_connection(connection).await;
}

pub async fn disconnected(connection: Arc<SocketConnection>) {
    let connection_id = connection.id;
    let connected_result = tokio::task::spawn(handle_disconnected(connection)).await;

    if let Err(err) = connected_result {
        println!(
            "Panic at handeling tcp disconnected event for connection {}. Error: {:?}",
            connection_id, err
        );
    }
}

async fn handle_disconnected(connection: Arc<SocketConnection>) {
    //TODO -  Remove if not needed
    //publisher.disconnect(connection.id).await;
}

pub async fn new_packet(contract: TcpContract, subscribers: &Subscribers) {
    match contract {
        TcpContract::Ping => {}
        TcpContract::Pong => {}
        TcpContract::Greeting { name: _ } => {}
        TcpContract::Subscribe { table_name: _ } => {}
        TcpContract::InitTable { table_name, data } => {
            if let Some(update_event) = subscribers.get(table_name.as_str()).await {
                update_event.as_ref().init_table(data).await;
            }
        }
        TcpContract::InitPartition {
            table_name,
            partition_key,
            data,
        } => {
            if let Some(update_event) = subscribers.get(table_name.as_str()).await {
                update_event
                    .as_ref()
                    .init_partition(partition_key.as_str(), data)
                    .await;
            }
        }
        TcpContract::UpdateRows { table_name, data } => {
            if let Some(update_event) = subscribers.get(table_name.as_str()).await {
                update_event.as_ref().update_rows(data).await;
            }
        }
        TcpContract::DeleteRows { table_name, rows } => {
            if let Some(update_event) = subscribers.get(table_name.as_str()).await {
                update_event.as_ref().delete_rows(rows).await;
            }
        }
    }
}
