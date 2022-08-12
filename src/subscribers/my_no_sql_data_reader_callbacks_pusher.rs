use std::sync::Arc;

use my_no_sql_server_abstractions::MyNoSqlEntity;
use rust_extensions::{
    events_loop::{EventsLoop, EventsLoopTick},
    ApplicationStates, Logger,
};

use super::MyNoSqlDataRaderCallBacks;

pub enum PusherEvents<TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static> {
    Added(String, Vec<Arc<TMyNoSqlEntity>>),
    Updated(String, Vec<Arc<TMyNoSqlEntity>>),
    Deleted(String, Vec<Arc<TMyNoSqlEntity>>),
}

pub struct MyNoSqlDataRaderCallBacksPusher<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
{
    events_loop: EventsLoop<PusherEvents<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataRaderCallBacksPusher<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
{
    pub async fn new<
        TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        callbacks: Arc<TMyNoSqlDataRaderCallBacks>,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        let events_loop_reader = MyNoSqlDataRaderCallBacksSender::new(callbacks, None);
        let events_loop = EventsLoop::new("MyNoSqlDataRaderCallBacksPusher".to_string());

        events_loop
            .register_event_loop(Arc::new(events_loop_reader))
            .await;

        events_loop.start(app_states, logger).await;
        Self { events_loop }
    }
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity> MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>
    for MyNoSqlDataRaderCallBacksPusher<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
{
    async fn added(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>) {
        self.events_loop
            .send(PusherEvents::Added(partition_key.to_string(), entities));
    }

    async fn updated(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>) {
        self.events_loop
            .send(PusherEvents::Updated(partition_key.to_string(), entities));
    }

    async fn deleted(&self, partition_key: &str, entities: Vec<Arc<TMyNoSqlEntity>>) {
        self.events_loop
            .send(PusherEvents::Deleted(partition_key.to_string(), entities));
    }
}

pub struct MyNoSqlDataRaderCallBacksSender<
    TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
    TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity>,
> {
    callbacks: Arc<TMyNoSqlDataRaderCallBacks>,
    item: Option<TMyNoSqlEntity>,
}

impl<
        TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
        TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    > MyNoSqlDataRaderCallBacksSender<TMyNoSqlEntity, TMyNoSqlDataRaderCallBacks>
{
    pub fn new(callbacks: Arc<TMyNoSqlDataRaderCallBacks>, item: Option<TMyNoSqlEntity>) -> Self {
        Self { callbacks, item }
    }
}

#[async_trait::async_trait]
impl<
        TMyNoSqlEntity: MyNoSqlEntity + Send + Sync + 'static,
        TMyNoSqlDataRaderCallBacks: MyNoSqlDataRaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    > EventsLoopTick<PusherEvents<TMyNoSqlEntity>>
    for MyNoSqlDataRaderCallBacksSender<TMyNoSqlEntity, TMyNoSqlDataRaderCallBacks>
{
    async fn tick(&self, model: PusherEvents<TMyNoSqlEntity>) {
        match model {
            PusherEvents::Added(partition_key, entities) => {
                self.callbacks.added(partition_key.as_str(), entities).await;
            }
            PusherEvents::Updated(partition_key, entities) => {
                self.callbacks
                    .updated(partition_key.as_str(), entities)
                    .await;
            }
            PusherEvents::Deleted(partition_key, entities) => {
                self.callbacks
                    .deleted(partition_key.as_str(), entities)
                    .await;
            }
        }
        if self.item.is_some() {}
    }
}
