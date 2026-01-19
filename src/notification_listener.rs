use std::time::Duration;

use anyhow::{
    Context as _,
    Result,
};
use serde::Deserialize;
use tokio::sync::{
    broadcast,
    mpsc,
    oneshot,
};

use crate::listener::service::ListenMessage;
use crate::listener::{
    Channel,
    Notification,
    TypedChannel,
    TypedNotification,
};

#[derive(Clone)]
pub struct NotificationListener {
    listen_tx: mpsc::Sender<ListenMessage>,
    unlisten_tx: mpsc::Sender<ListenMessage>,
    notification_tx: broadcast::Sender<Notification>,
}

impl NotificationListener {
    pub fn new(
        listen_tx: mpsc::Sender<ListenMessage>,
        unlisten_tx: mpsc::Sender<ListenMessage>,
        notification_tx: broadcast::Sender<Notification>,
    ) -> Self {
        Self {
            listen_tx,
            unlisten_tx,
            notification_tx,
        }
    }

    pub async fn listen(&self, channel: Channel) -> Result<ChannelGuard> {
        let (tx, rx) = oneshot::channel();

        self.listen_tx
            .send((channel.clone(), tx))
            .await
            .context("failed to send listen request")?;

        rx.await
            .context("listen service unavailable")?
            .context("failed to listen to channel")?;

        Ok(ChannelGuard {
            channel,
            unlisten_tx: self.unlisten_tx.clone(),
            receiver: self.notification_tx.subscribe(),
        })
    }

    pub async fn listen_typed<T>(&self, channel: TypedChannel<T>) -> Result<TypedChannelGuard<T>>
    where
        T: for<'de> Deserialize<'de> + Send + Sync,
    {
        let channel_guard = self.listen(channel.into()).await?;

        Ok(TypedChannelGuard {
            channel_guard,
            _phantom: std::marker::PhantomData,
        })
    }
}

pub struct TypedChannelGuard<T> {
    channel_guard: ChannelGuard,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> TypedChannelGuard<T>
where
    T: for<'de> Deserialize<'de> + Send + Sync + std::fmt::Debug,
{
    pub async fn recv(&mut self) -> Result<TypedNotification<T>, TypedRecvError> {
        let notification = self
            .channel_guard
            .recv()
            .await
            .map_err(TypedRecvError::Broadcast)?;

        let payload: T = serde_json::from_str(&notification.payload).map_err(|e| {
            TypedRecvError::Deserialize {
                channel: notification.channel.clone(),
                payload: notification.payload.clone(),
                error: e,
            }
        })?;

        Ok(TypedNotification {
            process_id: notification.process_id,
            channel: notification.channel,
            payload,
        })
    }
}

#[derive(Debug)]
pub enum TypedRecvError {
    Broadcast(broadcast::error::RecvError),
    Deserialize {
        channel: Channel,
        payload: String,
        error: serde_json::Error,
    },
}

pub struct ChannelGuard {
    channel: Channel,
    unlisten_tx: mpsc::Sender<ListenMessage>,
    receiver: broadcast::Receiver<Notification>,
}

impl ChannelGuard {
    pub async fn recv(&mut self) -> Result<Notification, broadcast::error::RecvError> {
        loop {
            let notification = self.receiver.recv().await?;

            if self.channel == notification.channel {
                return Ok(notification);
            }
        }
    }
}

impl Drop for ChannelGuard {
    fn drop(&mut self) {
        let unlisten_tx = self.unlisten_tx.clone();
        let channel = self.channel.clone();

        tokio::spawn(async move {
            loop {
                let (tx, rx) = oneshot::channel();

                if unlisten_tx.send((channel.clone(), tx)).await.is_err() {
                    break;
                }

                match rx.await {
                    Ok(Ok(())) | Err(_) => break,
                    Ok(Err(e)) => {
                        tracing::debug!("failed to unlisten from channel, trying again: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }
}
