use std::time::Duration;

use derive_more::{
    Deref,
    DerefMut,
};
use serde::Deserialize;
use tokio::sync::{
    broadcast,
    mpsc,
    oneshot,
    watch,
};

use crate::error::ListenError;
use crate::service::ListenerCommand;
use crate::{
    Channel,
    Notification,
    SubscriptionEvent,
    TypedChannel,
    TypedNotification,
};

#[derive(Clone)]
pub struct NotificationListener {
    command_tx: mpsc::Sender<ListenerCommand>,
    lifecycle: Option<watch::Receiver<bool>>,
}

impl NotificationListener {
    pub fn new(command_tx: mpsc::Sender<ListenerCommand>) -> Self {
        Self {
            command_tx,
            lifecycle: None,
        }
    }

    pub fn with_lifecycle(
        command_tx: mpsc::Sender<ListenerCommand>,
        lifecycle: watch::Receiver<bool>,
    ) -> Self {
        Self {
            command_tx,
            lifecycle: Some(lifecycle),
        }
    }

    pub async fn listen(&self, channel: Channel) -> Result<ChannelGuard, ListenError> {
        let request = async {
            let (tx, rx) = oneshot::channel();

            self.command_tx
                .send(ListenerCommand::Listen((channel.clone(), tx)))
                .await
                .map_err(|_| ListenError::ServiceClosed)?;

            let mut cleanup = PendingListenGuard::new(channel.clone(), self.command_tx.clone(), rx);

            let result = (&mut cleanup.response).await;
            cleanup.disarm();
            let result = result.map_err(|_| ListenError::ServiceUnavailable)?;

            let receiver = result.map_err(|source| ListenError::ListenFailed {
                channel: channel.clone(),
                source,
            })?;

            let mut lifecycle = self.lifecycle.clone();

            let initially_unavailable = lifecycle
                .as_mut()
                .is_some_and(|state| !*state.borrow_and_update());

            Ok(ChannelGuard {
                lifecycle,
                initially_unavailable,
                channel,
                command_tx: self.command_tx.clone(),
                receiver,
            })
        };

        tokio::time::timeout(Duration::from_secs(15), request)
            .await
            .map_err(|_| ListenError::ServiceUnavailable)?
    }

    pub async fn listen_typed<T>(
        &self,
        channel: TypedChannel<T>,
    ) -> Result<TypedChannelGuard<T>, ListenError>
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
        let notification = self.channel_guard.recv().await?;

        Self::deserialize(notification)
    }

    pub async fn recv_event(
        &mut self,
    ) -> Result<SubscriptionEvent<TypedNotification<T>>, TypedRecvError> {
        match self
            .channel_guard
            .recv_event()
            .await
            .map_err(TypedRecvError::Broadcast)?
        {
            SubscriptionEvent::Notification(notification) => {
                Self::deserialize(notification).map(SubscriptionEvent::Notification)
            }
            SubscriptionEvent::Unavailable => Ok(SubscriptionEvent::Unavailable),
            SubscriptionEvent::Restored => Ok(SubscriptionEvent::Restored),
        }
    }

    fn deserialize(notification: Notification) -> Result<TypedNotification<T>, TypedRecvError> {
        let payload = serde_json::from_str(&notification.payload).map_err(|error| {
            TypedRecvError::Deserialize {
                channel: notification.channel.clone(),
                payload: notification.payload.clone(),
                error,
            }
        })?;

        Ok(TypedNotification {
            process_id: notification.process_id,
            channel: notification.channel,
            payload,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TypedRecvError {
    #[error("failed to receive notification")]
    Broadcast(#[from] broadcast::error::RecvError),
    #[error("failed to deserialize notification on channel {channel}")]
    Deserialize {
        channel: Channel,
        payload: String,
        #[source]
        error: serde_json::Error,
    },
}

pub struct ChannelGuard {
    channel: Channel,
    command_tx: mpsc::Sender<ListenerCommand>,
    receiver: broadcast::Receiver<Notification>,
    lifecycle: Option<watch::Receiver<bool>>,
    initially_unavailable: bool,
}

struct PendingListenGuard {
    channel: Option<Channel>,
    command_tx: mpsc::Sender<ListenerCommand>,
    response: oneshot::Receiver<Result<broadcast::Receiver<Notification>, sqlx::Error>>,
}

impl PendingListenGuard {
    fn new(
        channel: Channel,
        command_tx: mpsc::Sender<ListenerCommand>,
        response: oneshot::Receiver<Result<broadcast::Receiver<Notification>, sqlx::Error>>,
    ) -> Self {
        Self {
            channel: Some(channel),
            command_tx,
            response,
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

    /// Observe delivery outages even when there are no channel notifications.
    /// Lifecycle changes take precedence over payloads and discard obsolete
    /// buffered notifications before asking the consumer to reconcile.
    pub async fn recv_event(
        &mut self,
    ) -> Result<SubscriptionEvent<Notification>, broadcast::error::RecvError> {
        if std::mem::take(&mut self.initially_unavailable) {
            return Ok(SubscriptionEvent::Unavailable);
        }

        if let Some(lifecycle) = self.lifecycle.as_mut() {
            tokio::select! {
                biased;
                changed = lifecycle.changed() => {
                    changed.map_err(|_| broadcast::error::RecvError::Closed)?;
                    self.receiver = self.receiver.resubscribe();
                    if *lifecycle.borrow_and_update() {
                        Ok(SubscriptionEvent::Restored)
                    } else {
                        Ok(SubscriptionEvent::Unavailable)
                    }
                }
                notification = self.receiver.recv() => notification.map(SubscriptionEvent::Notification),
            }
        } else {
            self.receiver
                .recv()
                .await
                .map(SubscriptionEvent::Notification)
        }
    }

    /// Discard payload backlog without discarding pending lifecycle changes.
    pub fn drain(&mut self) {
        self.receiver = self.receiver.resubscribe();
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
            // Closing the acknowledgement prevents the service from installing
            // a reference for a cancelled request. Only an already-acknowledged
            // success owns a reference that needs compensating cleanup.
            self.response.close();
            if matches!(self.response.try_recv(), Ok(Ok(_))) {
                spawn_unlisten(self.command_tx.clone(), channel);
            }
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
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn cancelled_listen_before_ack_closes_the_request_without_releasing_another_reference() {
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let listener = NotificationListener::new(command_tx);
        let channel = Channel::try_from("cancelled_before_ack".to_owned()).unwrap();
        {
            let pending = listener.listen(channel);
            tokio::pin!(pending);
            assert!(futures::poll!(pending.as_mut()).is_pending());
        }

        let ListenerCommand::Listen((_, response)) = command_rx.recv().await.unwrap() else {
            panic!("expected listen");
        };

        assert!(response.is_closed());
        assert!(
            timeout(Duration::from_millis(100), command_rx.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cancelled_listen_after_ack_releases_exactly_one_reference() {
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let listener = NotificationListener::new(command_tx);
        let channel = Channel::try_from("cancelled_after_ack".to_owned()).unwrap();
        {
            let pending = listener.listen(channel.clone());
            tokio::pin!(pending);
            assert!(futures::poll!(pending.as_mut()).is_pending());

            let ListenerCommand::Listen((_, response)) = command_rx.recv().await.unwrap() else {
                panic!("expected listen");
            };

            let (_sender, receiver) = broadcast::channel(1);
            response.send(Ok(receiver)).unwrap();
        }
        assert!(
            matches!(timeout(Duration::from_millis(100), command_rx.recv()).await.unwrap().unwrap(), ListenerCommand::Unlisten((released, _)) if released == channel)
        );
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
        let _ = response_tx.send(Err(sqlx::Error::PoolClosed));

        let Err(error) = listen_task.await.unwrap() else {
            panic!("listen unexpectedly succeeded")
        };
        assert!(matches!(error, ListenError::ListenFailed { .. }));
        assert!(matches!(
            timeout(Duration::from_millis(100), command_rx.recv()).await,
            Err(_) | Ok(None)
        ));
    }

    #[tokio::test]
    async fn notification_only_receivers_preserve_payloads_and_lag_after_restoration() {
        let (command_tx, _commands) = mpsc::channel(2);
        let (sender, receiver) = broadcast::channel(2);
        let (available, lifecycle) = watch::channel(true);
        let channel = Channel::try_from("notification_only_recovery".to_owned()).unwrap();

        let mut raw = ChannelGuard {
            channel: channel.clone(),
            command_tx: command_tx.clone(),
            receiver,
            lifecycle: Some(lifecycle.clone()),
            initially_unavailable: false,
        };

        let mut typed = TypedChannelGuard::<String> {
            channel_guard: ChannelGuard {
                channel: channel.clone(),
                command_tx,
                receiver: sender.subscribe(),
                lifecycle: Some(lifecycle),
                initially_unavailable: false,
            },
            _phantom: std::marker::PhantomData,
        };

        available.send(false).unwrap();
        available.send(true).unwrap();

        for payload in ["first", "second", "third"] {
            sender
                .send(Notification {
                    process_id: 1,
                    channel: channel.clone(),
                    payload: serde_json::to_string(payload).unwrap(),
                })
                .unwrap();
        }

        assert!(matches!(
            timeout(Duration::from_secs(1), raw.recv()).await.unwrap(),
            Err(broadcast::error::RecvError::Lagged(1))
        ));

        assert!(matches!(
            timeout(Duration::from_secs(1), typed.recv()).await.unwrap(),
            Err(TypedRecvError::Broadcast(
                broadcast::error::RecvError::Lagged(1)
            ))
        ));

        for expected in ["second", "third"] {
            assert_eq!(
                raw.recv().await.unwrap().payload,
                serde_json::to_string(expected).unwrap()
            );

            assert_eq!(typed.recv().await.unwrap().payload, expected);
        }

        // Notification-only reads must not consume the recovery signal either.
        assert!(matches!(
            raw.recv_event().await.unwrap(),
            SubscriptionEvent::Restored
        ));

        assert!(matches!(
            typed.recv_event().await.unwrap(),
            SubscriptionEvent::Restored
        ));
    }

    #[tokio::test]
    async fn lifecycle_survives_lag_and_drain_and_discards_stale_payloads() {
        let (command_tx, _commands) = mpsc::channel(2);
        let (sender, receiver) = broadcast::channel(2);
        let (available, lifecycle) = watch::channel(true);
        let channel = Channel::try_from("lifecycle_lag".to_owned()).unwrap();

        let mut guard = ChannelGuard {
            channel: channel.clone(),
            command_tx,
            receiver,
            lifecycle: Some(lifecycle),
            initially_unavailable: false,
        };

        for payload in [
            "old_notification",
            "later_notification",
            "superseded_notification",
        ] {
            sender
                .send(Notification {
                    process_id: 1,
                    channel: channel.clone(),
                    payload: payload.to_owned(),
                })
                .unwrap();
        }

        assert!(matches!(
            guard.receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));

        available.send(false).unwrap();
        guard.drain();

        assert!(matches!(
            guard.recv_event().await.unwrap(),
            SubscriptionEvent::Unavailable
        ));

        sender
            .send(Notification {
                process_id: 1,
                channel: channel.clone(),
                payload: "obsolete".to_owned(),
            })
            .unwrap();

        available.send(true).unwrap();

        assert!(matches!(
            guard.recv_event().await.unwrap(),
            SubscriptionEvent::Restored
        ));

        assert!(matches!(
            guard.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        sender
            .send(Notification {
                process_id: 1,
                channel,
                payload: "fresh".to_owned(),
            })
            .unwrap();

        assert!(
            matches!(guard.recv_event().await.unwrap(), SubscriptionEvent::Notification(notification) if notification.payload == "fresh")
        );
    }

    #[tokio::test]
    async fn coalesced_outage_still_reports_restoration() {
        let (command_tx, _commands) = mpsc::channel(2);
        let (_sender, receiver) = broadcast::channel(2);
        let (available, lifecycle) = watch::channel(true);

        let mut guard = ChannelGuard {
            channel: Channel::try_from("coalesced_outage".to_owned()).unwrap(),
            command_tx,
            receiver,
            lifecycle: Some(lifecycle),
            initially_unavailable: false,
        };

        available.send(false).unwrap();
        available.send(true).unwrap();

        assert!(matches!(
            guard.recv_event().await.unwrap(),
            SubscriptionEvent::Restored
        ));
    }
}
