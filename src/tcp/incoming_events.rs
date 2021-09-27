use my_no_sql_tcp_shared::TcpContract;

use crate::subscribers::Subscribers;

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
