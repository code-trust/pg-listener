use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{
    Result,
    ensure,
};
use sqlx::postgres::PgListener;
use tokio::sync::{
    RwLock,
    broadcast,
    mpsc,
    oneshot,
};
use tokio_graceful_shutdown::{
    SubsystemBuilder,
    SubsystemHandle,
};

use crate::listener::{
    Channel,
    Notification,
    NotificationListener,
};

pub type ListenMessage = (Channel, oneshot::Sender<Result<()>>);

pub enum ListenerCommand {
    Listen(ListenMessage),
    Unlisten(ListenMessage),
}

impl ListenerCommand {
    async fn handle(
        self,
        listener: &mut PgListener,
        channel_refs: &Arc<RwLock<HashMap<Channel, usize>>>,
    ) {
        match self {
            Self::Listen((channel, response_tx)) => {
                ListenerService::handle_listen_request(
                    listener,
                    channel_refs,
                    channel,
                    response_tx,
                )
                .await;
            }
            Self::Unlisten((channel, response_tx)) => {
                ListenerService::handle_unlisten_request(
                    listener,
                    channel_refs,
                    channel,
                    response_tx,
                )
                .await;
            }
        }
    }
}

#[derive(Debug)]
pub struct ListenerService {
    pg_listener: PgListener,
    command_tx: mpsc::Sender<ListenerCommand>,
    command_rx: mpsc::Receiver<ListenerCommand>,
    notification_tx: broadcast::Sender<Notification>,
    channel_refs: Arc<RwLock<HashMap<Channel, usize>>>,
}

impl ListenerService {
    pub async fn try_new(pool: &sqlx::PgPool) -> Result<Self> {
        let pg_listener = PgListener::connect_with(pool).await?;
        let (command_tx, command_rx) = mpsc::channel::<ListenerCommand>(1024);
        let (tx, _rx) = broadcast::channel(1024);

        Ok(Self {
            pg_listener,
            command_tx,
            command_rx,
            notification_tx: tx,
            channel_refs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn notification_listener(&self) -> NotificationListener {
        NotificationListener::new(self.command_tx.clone(), self.notification_tx.clone())
    }

    pub async fn channel_ref_count(&self, channel: &Channel) -> usize {
        let refs = self.channel_refs.read().await;
        *refs.get(channel).unwrap_or(&0)
    }

    pub fn channel_refs(&self) -> Arc<RwLock<HashMap<Channel, usize>>> {
        self.channel_refs.clone()
    }

    pub fn start(self, parent: &SubsystemHandle) {
        let mut listener = self.pg_listener;
        let tx = self.notification_tx;
        let mut command_rx = self.command_rx;
        let channel_refs = self.channel_refs;

        parent.start(SubsystemBuilder::new(
            "listener",
            async move |subsys: &mut SubsystemHandle| {
                tracing::info!("starting notification broadcaster");

                loop {
                    tokio::select! {
                        () = subsys.on_shutdown_requested() => {
                            tracing::info!("shutdown requested for listener");
                            break;
                        }
                        Some(command) = command_rx.recv() => command.handle(&mut listener, &channel_refs).await,
                        result = Self::handle_notification(&mut listener, &tx) => {
                            if let Err(e) = result {
                                tracing::error!("Notification handling error: {e}");
                            }
                        }
                    }
                }

                ensure!(
                    subsys.is_shutdown_requested(),
                    "returned without a shutdown request"
                );

                Ok(())
            },
        ));
    }

    async fn handle_listen_request(
        listener: &mut PgListener,
        channel_refs: &Arc<RwLock<HashMap<Channel, usize>>>,
        channel: Channel,
        response_tx: oneshot::Sender<Result<()>>,
    ) {
        if let Some(count) = channel_refs.write().await.get_mut(&channel) {
            *count += 1;
            tracing::info!("Channel {} subscription count: {}", channel, count);
            let _ = response_tx.send(Ok(()));

            return;
        }

        if let Err(e) = listener.listen(channel.as_ref()).await {
            tracing::error!("Failed to listen to channel {}: {}", channel, e);
            let _ = response_tx.send(Err(e.into()));
            return;
        }

        tracing::info!("Now listening to channel: {} (refs: 1)", channel);
        channel_refs.write().await.insert(channel.clone(), 1);
        let _ = response_tx.send(Ok(()));
    }

    async fn handle_unlisten_request(
        listener: &mut PgListener,
        channel_refs: &Arc<RwLock<HashMap<Channel, usize>>>,
        channel: Channel,
        response_tx: oneshot::Sender<Result<()>>,
    ) {
        match channel_refs.write().await.get_mut(&channel) {
            None => {
                tracing::debug!("ignoring unlisten for inactive channel: {channel}");
                let _ = response_tx.send(Ok(()));
                return;
            }
            Some(0) => unreachable!(),
            Some(1) => {}
            Some(count) => {
                *count -= 1;

                tracing::info!(
                    "Channel {} subscription count: {} (still active)",
                    channel,
                    count
                );

                let _ = response_tx.send(Ok(()));
                return;
            }
        }

        if let Err(e) = listener.unlisten(channel.as_ref()).await {
            tracing::error!("Failed to unlisten from channel {}: {}", channel, e);
            let _ = response_tx.send(Err(e.into()));
            return;
        }

        tracing::info!("Stopped listening to channel: {}", channel);
        channel_refs.write().await.remove(&channel);
        let _ = response_tx.send(Ok(()));
    }

    async fn handle_notification(
        listener: &mut PgListener,
        tx: &broadcast::Sender<Notification>,
    ) -> Result<()> {
        let notification = listener.recv().await?;
        let _ = tx.send(notification.into());
        Ok(())
    }
}
