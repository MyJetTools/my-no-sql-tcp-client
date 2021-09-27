use my_no_sql_tcp_shared::TcpContract;

use crate::subscribers::Subscribers;

use super::SocketConnection;

pub async fn send_init(socket_ctx: &SocketConnection, app_name: &str, subscribers: &Subscribers) {
    send_greeting(socket_ctx, app_name).await;
    subscribe_to_tables(socket_ctx, subscribers).await;
}

pub async fn send_greeting(socket_ctx: &SocketConnection, app_name: &str) {
    let contract = TcpContract::Greeting {
        name: app_name.to_string(),
    };

    socket_ctx
        .send_data_to_socket_and_forget(contract.serialize().as_slice())
        .await;
}

pub async fn subscribe_to_tables(socket_ctx: &SocketConnection, subscribers: &Subscribers) {
    for table in subscribers.get_tables_to_subscirbe().await {
        let contract = TcpContract::Subscribe {
            table_name: table.to_string(),
        };

        socket_ctx
            .send_data_to_socket_and_forget(contract.serialize().as_slice())
            .await;
    }
}
