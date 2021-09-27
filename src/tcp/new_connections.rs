use std::{sync::Arc, time::Duration};

use tokio::net::TcpStream;

use tokio::io::{self, ReadHalf};

use crate::logger::MyLogger;
use crate::subscribers::Subscribers;

use super::SocketConnection;

pub async fn start(
    logger: Arc<MyLogger>,
    host_port: String,
    app_name: String,
    ping_timeout: Duration,
    connect_timeout: Duration,
    subscribers: Arc<Subscribers>,
) {
    let mut socket_id = 0;
    loop {
        tokio::time::sleep(connect_timeout).await;
        socket_id += 1;

        let connect_result = TcpStream::connect(host_port.as_str()).await;

        match connect_result {
            Ok(tcp_stream) => {
                let (read_socket, write_socket) = io::split(tcp_stream);

                let socket_connection = SocketConnection::new(socket_id, write_socket);

                let socket_connection = Arc::new(socket_connection);

                super::incoming_events::connected(socket_connection.clone()).await;

                process_new_connection(
                    logger.clone(),
                    socket_connection.clone(),
                    ping_timeout,
                    read_socket,
                    app_name.as_str(),
                    subscribers.clone(),
                )
                .await;

                super::incoming_events::disconnected(socket_connection).await;
            }
            Err(err) => {
                logger.write_log(
                    crate::logger::LogType::Error,
                    "Connect loop".to_string(),
                    format!(
                        "Can not connect to the socket {}. Err: {:?}",
                        host_port, err
                    ),
                    None,
                );
            }
        }
    }
}

async fn process_new_connection(
    logger: Arc<MyLogger>,
    socket_connection: Arc<SocketConnection>,
    ping_timeout: Duration,
    read_socket: ReadHalf<TcpStream>,
    app_name: &str,
    subscribers: Arc<Subscribers>,
) {
    let read_task = tokio::task::spawn(super::read_loop::start_new(
        read_socket,
        socket_connection.clone(),
        app_name.to_string(),
        subscribers.clone(),
    ));

    super::ping_loop::start_new(logger.clone(), socket_connection.clone(), ping_timeout).await;

    let read_result = read_task.await;
    socket_connection.disconnect().await;

    if let Err(err) = read_result {
        logger.write_log(
            crate::logger::LogType::Error,
            format!("Connection process {}", socket_connection.id),
            format!(
                "We have error exiting the read loop for the client socket {}",
                socket_connection.id,
            ),
            Some(format!("{:?}", err)),
        );
    }
}
