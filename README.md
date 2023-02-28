# How to use


## 1. Make settings structure and implement MyNoSqlTcpConnectionSettings trait.

Basically Connection read host_port each recoonection moment. Trait gives ability to update host_port in realtime without restarting the application.
```rust
pub struct MyNoSqlTcpReaderSettings {}

#[async_trait::async_trait]
impl my_no_sql_tcp_reader::MyNoSqlTcpConnectionSettings for MyNoSqlTcpReaderSettings {
    async fn get_host_port(&self) -> String {
        "localhost:5125".to_string()
    }
}

```

## 2. Create entity
The Serde and https://github.com/MyJetTools/my-no-sql-macros macros libraries are used.


Without expiration
```rust
#[my_no_sql_macros::my_no_sql_entity("test")]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestEntity {
}

```

With expiration
```rust
#[my_no_sql_macros::my_no_sql_entity("test")]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestEntity {
    #[serde(rename = "Expires")]
    expires: String,
}
```

## 3. Create connection, exctract reader from the connection and start the connection;

```rust
    let connection = my_no_sql_tcp_reader::MyNoSqlTcpConnection::new(
        "app_name".to_string(),
        Arc::new(MyNoSqlTcpReaderSettings {}),
    );

    let reader: Arc<MyNoSqlDataReader<TestEntity>> = connection.get_reader().await;
    
    connection.start(my_logger::LOGGER.clone()).await;
```

