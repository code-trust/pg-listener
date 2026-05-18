use std::time::Duration;

use anyhow::{
    Context as _,
    Result,
};
use derive_more::{
    Deref,
    DerefMut,
};
use serde::Deserialize;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{
    broadcast,
    mpsc,
    oneshot,
};

use crate::listener::service::ListenerCommand;
use crate::listener::{
    Channel,
    Notification,
    TypedChannel,
    TypedNotification,
};

#[derive(Clone)]
pub struct NotificationListener {
    command_tx: mpsc::Sender<ListenerCommand>,
}

impl NotificationListener {
    pub fn new(command_tx: mpsc::Sender<ListenerCommand>) -> Self {
        Self { command_tx }
    }

    pub async fn listen(&self, channel: Channel) -> Result<ChannelGuard> {
        let (tx, rx) = oneshot::channel();

        self.command_tx
            .send(ListenerCommand::Listen((channel.clone(), tx)))
            .await
            .context("failed to send listen request")?;

        let mut cleanup = PendingListenGuard::new(channel.clone(), self.command_tx.clone());

        let result = rx.await.context("listen service unavailable")?;
        cleanup.disarm();
        let receiver = result.context("failed to listen to channel")?;

        Ok(ChannelGuard {
            channel,
            command_tx: self.command_tx.clone(),
            receiver,
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

#[derive(Deref, DerefMut)]
pub struct TypedChannelGuard<T> {
    #[deref]
    #[deref_mut]
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
    command_tx: mpsc::Sender<ListenerCommand>,
    receiver: broadcast::Receiver<Notification>,
}

struct PendingListenGuard {
    channel: Option<Channel>,
    command_tx: mpsc::Sender<ListenerCommand>,
}

impl PendingListenGuard {
    fn new(channel: Channel, command_tx: mpsc::Sender<ListenerCommand>) -> Self {
        Self {
            channel: Some(channel),
            command_tx,
        }
    }

    fn disarm(&mut self) {
        self.channel = None;
    }
}

impl ChannelGuard {
    pub async fn recv(&mut self) -> Result<Notification, broadcast::error::RecvError> {
        self.receiver.recv().await
    }

    pub fn drain(&mut self) {
        while let Ok(_) | Err(TryRecvError::Lagged(_)) = self.receiver.try_recv() {}
    }
}

impl Drop for ChannelGuard {
    fn drop(&mut self) {
        spawn_unlisten(self.command_tx.clone(), self.channel.clone());
    }
}

impl Drop for PendingListenGuard {
    fn drop(&mut self) {
        if let Some(channel) = self.channel.take() {
            spawn_unlisten(self.command_tx.clone(), channel);
        }
    }
}

fn spawn_unlisten(command_tx: mpsc::Sender<ListenerCommand>, channel: Channel) {
    tokio::spawn(async move {
        loop {
            let (tx, rx) = oneshot::channel();

            if command_tx
                .send(ListenerCommand::Unlisten((channel.clone(), tx)))
                .await
                .is_err()
            {
                return;
            }

            let Ok(Err(error)) = rx.await else {
                return;
            };

            tracing::debug!("failed to unlisten from channel, trying again: {error}");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn cancelled_listen_before_ack_enqueues_release_after_listen() {
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let listener = NotificationListener::new(command_tx);
        let channel = Channel::try_from("cancelled_before_ack".to_owned()).unwrap();

        {
            let cancelled_listen = listener.listen(channel);
            tokio::pin!(cancelled_listen);

            assert!(futures::poll!(cancelled_listen.as_mut()).is_pending());
        }

        let ListenerCommand::Listen((received_channel, _response_tx)) =
            command_rx.recv().await.unwrap()
        else {
            panic!("expected listen command");
        };
        assert_eq!(received_channel.as_ref(), "cancelled_before_ack");

        let ListenerCommand::Unlisten((received_channel, _response_tx)) =
            timeout(Duration::from_millis(100), command_rx.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected unlisten command");
        };
        assert_eq!(received_channel.as_ref(), "cancelled_before_ack");
    }

    #[tokio::test]
    async fn failed_listen_does_not_queue_unlisten_cleanup() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let listener = NotificationListener::new(command_tx);
        let channel = Channel::try_from("failed_listen_cleanup".to_owned()).unwrap();
        let expected_channel = channel.clone();

        let listen_task = tokio::spawn(async move { listener.listen(channel).await });

        let ListenerCommand::Listen((received_channel, response_tx)) =
            command_rx.recv().await.unwrap()
        else {
            panic!("expected listen command");
        };
        assert_eq!(received_channel, expected_channel);
        let _ = response_tx.send(Err(anyhow!("boom")));

        let Err(error) = listen_task.await.unwrap() else {
            panic!("listen unexpectedly succeeded")
        };
        assert!(error.to_string().contains("failed to listen to channel"));
        assert!(matches!(
            timeout(Duration::from_millis(100), command_rx.recv()).await,
            Err(_) | Ok(None)
        ));
    }
}
