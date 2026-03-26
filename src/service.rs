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

const NOTIFICATION_CHANNEL_CAPACITY: usize = 128;

pub type ListenMessage = (
    Channel,
    oneshot::Sender<Result<broadcast::Receiver<Notification>>>,
);
pub type UnlistenMessage = (Channel, oneshot::Sender<Result<()>>);

pub enum ListenerCommand {
    Listen(ListenMessage),
    Unlisten(UnlistenMessage),
}

impl ListenerCommand {
    async fn handle(self, listener: &mut PgListener, subscriptions: &ListenerSubscriptions) {
        match self {
            Self::Listen((channel, response_tx)) => {
                ListenerService::handle_listen_request(
                    listener,
                    subscriptions,
                    channel,
                    response_tx,
                )
                .await;
            }
            Self::Unlisten((channel, response_tx)) => {
                ListenerService::handle_unlisten_request(
                    listener,
                    subscriptions,
                    channel,
                    response_tx,
                )
                .await;
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ListenerSubscriptions {
    inner: Arc<RwLock<HashMap<Channel, ChannelSubscription>>>,
}

#[derive(Debug)]
struct ChannelSubscription {
    ref_count: usize,
    tx: broadcast::Sender<Notification>,
}

impl ListenerSubscriptions {
    pub async fn channel_ref_count(&self, channel: &Channel) -> usize {
        self.inner
            .read()
            .await
            .get(channel)
            .map_or(0, |subscription| subscription.ref_count)
    }

    async fn subscribe(
        &self,
        listener: &mut PgListener,
        channel: Channel,
    ) -> Result<broadcast::Receiver<Notification>> {
        if let Some(subscription) = self.inner.write().await.get_mut(&channel) {
            subscription.ref_count += 1;

            tracing::info!(
                "Channel {} subscription count: {}",
                channel,
                subscription.ref_count
            );

            return Ok(subscription.tx.subscribe());
        }

        listener.listen(channel.as_ref()).await?;

        let (tx, rx) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
        tracing::info!("Now listening to channel: {} (refs: 1)", channel);

        self.inner
            .write()
            .await
            .insert(channel, ChannelSubscription { ref_count: 1, tx });

        Ok(rx)
    }

    async fn unsubscribe(&self, listener: &mut PgListener, channel: Channel) -> Result<()> {
        {
            match self.inner.write().await.get_mut(&channel) {
                None => {
                    tracing::debug!("ignoring unlisten for inactive channel: {channel}");
                    return Ok(());
                }
                Some(subscription) if subscription.ref_count > 1 => {
                    subscription.ref_count -= 1;

                    tracing::info!(
                        "Channel {} subscription count: {} (still active)",
                        channel,
                        subscription.ref_count
                    );

                    return Ok(());
                }
                Some(_) => {}
            }
        }

        listener.unlisten(channel.as_ref()).await?;
        tracing::info!("Stopped listening to channel: {}", channel);
        self.inner.write().await.remove(&channel);

        Ok(())
    }

    async fn dispatch(&self, notification: Notification) {
        let tx = self
            .inner
            .read()
            .await
            .get(&notification.channel)
            .map(|subscription| subscription.tx.clone());

        if let Some(tx) = tx {
            let _ = tx.send(notification);
        }
    }
}

#[derive(Debug)]
pub struct ListenerService {
    pg_listener: PgListener,
    command_tx: mpsc::Sender<ListenerCommand>,
    command_rx: mpsc::Receiver<ListenerCommand>,
    subscriptions: ListenerSubscriptions,
}

impl ListenerService {
    pub async fn try_new(pool: &sqlx::PgPool) -> Result<Self> {
        let pg_listener = PgListener::connect_with(pool).await?;
        let (command_tx, command_rx) = mpsc::channel::<ListenerCommand>(1024);

        Ok(Self {
            pg_listener,
            command_tx,
            command_rx,
            subscriptions: ListenerSubscriptions::default(),
        })
    }

    pub fn notification_listener(&self) -> NotificationListener {
        NotificationListener::new(self.command_tx.clone())
    }

    pub async fn channel_ref_count(&self, channel: &Channel) -> usize {
        self.subscriptions.channel_ref_count(channel).await
    }

    pub fn subscriptions(&self) -> ListenerSubscriptions {
        self.subscriptions.clone()
    }

    pub fn start(mut self, parent: &SubsystemHandle) {
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
                        Some(command) = self.command_rx.recv() => command.handle(&mut self.pg_listener, &self.subscriptions).await,
                        result = Self::handle_notification(&mut self.pg_listener, &self.subscriptions) => {
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
        subscriptions: &ListenerSubscriptions,
        channel: Channel,
        response_tx: oneshot::Sender<Result<broadcast::Receiver<Notification>>>,
    ) {
        match subscriptions.subscribe(listener, channel.clone()).await {
            Ok(receiver) => {
                let _ = response_tx.send(Ok(receiver));
            }
            Err(e) => {
                tracing::error!("Failed to listen to channel {}: {}", channel, e);
                let _ = response_tx.send(Err(e));
            }
        }
    }

    async fn handle_unlisten_request(
        listener: &mut PgListener,
        subscriptions: &ListenerSubscriptions,
        channel: Channel,
        response_tx: oneshot::Sender<Result<()>>,
    ) {
        if let Err(e) = subscriptions.unsubscribe(listener, channel.clone()).await {
            tracing::error!("Failed to unlisten from channel {}: {}", channel, e);
            let _ = response_tx.send(Err(e));
            return;
        }
        let _ = response_tx.send(Ok(()));
    }

    async fn handle_notification(
        listener: &mut PgListener,
        subscriptions: &ListenerSubscriptions,
    ) -> Result<()> {
        let notification = listener.recv().await?;
        subscriptions.dispatch(notification.into()).await;
        Ok(())
    }
}
