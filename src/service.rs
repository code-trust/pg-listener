use std::collections::{
    HashMap,
    HashSet,
    VecDeque,
};
use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::{
    PgConnectOptions,
    PgListener,
    PgPoolOptions,
};
use tokio::sync::{
    RwLock,
    broadcast,
    mpsc,
    oneshot,
    watch,
};
use tokio_graceful_shutdown::{
    SubsystemBuilder,
    SubsystemHandle,
};

use crate::error::ListenerError;
use crate::{
    Channel,
    Notification,
    NotificationListener,
};

const NOTIFICATION_CHANNEL_CAPACITY: usize = 128;
const COMMAND_BATCH_SIZE: usize = 64;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
const SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
const HEALTH_INTERVAL: Duration = Duration::from_secs(15);

pub type ListenMessage = (
    Channel,
    oneshot::Sender<Result<broadcast::Receiver<Notification>, sqlx::Error>>,
);
pub type UnlistenMessage = (Channel, oneshot::Sender<Result<(), sqlx::Error>>);

#[derive(Debug)]
pub enum ListenerCommand {
    Listen(ListenMessage),
    Unlisten(UnlistenMessage),
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

    async fn channels(&self) -> Vec<Channel> {
        self.inner.read().await.keys().cloned().collect()
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
    options: PgConnectOptions,
    pg_listener: Option<PgListener>,
    command_tx: mpsc::Sender<ListenerCommand>,
    command_rx: mpsc::Receiver<ListenerCommand>,
    pending: VecDeque<ListenerCommand>,
    cleanup: HashSet<Channel>,
    subscriptions: ListenerSubscriptions,
    lifecycle_tx: watch::Sender<bool>,
    lifecycle_rx: watch::Receiver<bool>,
}

impl ListenerService {
    pub async fn try_new(pool: &sqlx::PgPool) -> Result<Self, ListenerError> {
        let options = (*pool.connect_options()).clone();

        let pg_listener = Self::connect(&options)
            .await
            .map_err(ListenerError::Connect)?;

        let (command_tx, command_rx) = mpsc::channel(1024);
        let (lifecycle_tx, lifecycle_rx) = watch::channel(true);

        Ok(Self {
            options,
            pg_listener: Some(pg_listener),
            command_tx,
            command_rx,
            pending: VecDeque::new(),
            cleanup: HashSet::new(),
            subscriptions: ListenerSubscriptions::default(),
            lifecycle_tx,
            lifecycle_rx,
        })
    }

    pub fn notification_listener(&self) -> NotificationListener {
        NotificationListener::with_lifecycle(self.command_tx.clone(), self.lifecycle_rx.clone())
    }

    pub async fn channel_ref_count(&self, channel: &Channel) -> usize {
        self.subscriptions.channel_ref_count(channel).await
    }

    pub fn subscriptions(&self) -> ListenerSubscriptions {
        self.subscriptions.clone()
    }

    async fn connect(options: &PgConnectOptions) -> Result<PgListener, sqlx::Error> {
        // PgListener's asynchronous UNLISTEN cleanup can retain a pool slot on
        // a half-open connection. A fresh dedicated pool cannot be blocked by
        // cleanup from an earlier connection.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy_with(options.clone());

        let mut listener =
            tokio::time::timeout(CONNECTION_TIMEOUT, PgListener::connect_with(&pool))
                .await
                .map_err(|_| timed_out())??;

        listener.eager_reconnect(false);

        Ok(listener)
    }

    pub fn start(mut self, parent: &SubsystemHandle) {
        parent.start(SubsystemBuilder::new("listener", async move |subsys: &mut SubsystemHandle| {
            loop {
                tokio::select! {
                    () = subsys.on_shutdown_requested() => return Ok::<(), ListenerError>(()),
                    result = self.run_connected() => {
                        tracing::warn!(?result, "Notification connection lost; restoring subscriptions");

                        self.lifecycle_tx.send_if_modified(std::mem::take);
                    }
                }

                tokio::select! {
                    () = subsys.on_shutdown_requested() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(3)) => {},
                }
            }
        }));
    }

    async fn run_connected(&mut self) -> Result<(), sqlx::Error> {
        let mut listener = match self.pg_listener.take() {
            Some(listener) => listener,
            None => Self::connect(&self.options).await?,
        };

        // A new connection has no obsolete subscriptions to clean up.
        self.cleanup.clear();
        self.restore_subscriptions(&mut listener).await?;
        self.forward(&mut listener).await
    }

    async fn restore_subscriptions(&self, listener: &mut PgListener) -> Result<(), sqlx::Error> {
        let channels = self.subscriptions.channels().await;

        if !channels.is_empty() {
            tokio::time::timeout(
                SUBSCRIPTION_TIMEOUT,
                listener.listen_all(channels.iter().map(AsRef::as_ref)),
            )
            .await
            .map_err(|_| timed_out())??;
        }

        // Notifications buffered by subscription SQL precede the catch-up
        // snapshot. Do not replay them after signaling restoration.
        while listener.next_buffered().is_some() {}

        // Only now are all active channels live. The watch version preserves
        // restoration even if a consumer misses a short unavailable interval.
        self.lifecycle_tx
            .send_if_modified(|available| !std::mem::replace(available, true));

        Ok(())
    }

    async fn forward(&mut self, listener: &mut PgListener) -> Result<(), sqlx::Error> {
        let mut health = tokio::time::interval(HEALTH_INTERVAL);
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if let Some(command) = self.pending.pop_front() {
                self.apply_commands(listener, command).await?;
                continue;
            }

            tokio::select! {
                _ = health.tick() => {
                    // This schema-independent query must compile without a database.
                    tokio::time::timeout(HEALTH_TIMEOUT, sqlx::query("SELECT 1").execute(&mut *listener)).await.map_err(|_| timed_out())??;
                    self.dispatch_buffered(listener).await;
                }
                Some(command) = self.command_rx.recv() => {
                    self.apply_commands(listener, command).await?;
                }
                notification = listener.try_recv() => {
                    let notification = notification?.ok_or_else(|| {
                        sqlx::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "notification connection interrupted",
                        ))
                    })?;

                    self.subscriptions.dispatch(notification.into()).await;
                }
                () = std::future::ready(()), if !self.cleanup.is_empty() => {
                    let channel = self.cleanup.iter().next().expect("cleanup is pending").clone();
                    self.cleanup.remove(&channel);

                    tokio::time::timeout(SUBSCRIPTION_TIMEOUT, listener.unlisten(channel.as_ref()))
                        .await.map_err(|_| timed_out())??;

                    self.dispatch_buffered(listener).await;
                }
            }
        }
    }

    async fn dispatch_buffered(&self, listener: &mut PgListener) {
        while let Some(notification) = listener.next_buffered() {
            self.subscriptions.dispatch(notification.into()).await;
        }
    }

    async fn apply_commands(
        &mut self,
        listener: &mut PgListener,
        first: ListenerCommand,
    ) -> Result<(), sqlx::Error> {
        let commands: Vec<_> = std::iter::once(first)
            .chain(std::iter::from_fn(|| {
                self.pending
                    .pop_front()
                    .or_else(|| self.command_rx.try_recv().ok())
            }))
            .take(COMMAND_BATCH_SIZE)
            .collect();

        let existing: HashSet<_> = self.subscriptions.channels().await.into_iter().collect();

        let additions: HashSet<_> = commands
            .iter()
            .filter_map(|command| match command {
                ListenerCommand::Listen((channel, response))
                    if !response.is_closed() && !existing.contains(channel) =>
                {
                    Some(channel.clone())
                }
                _ => None,
            })
            .collect();

        let sql_additions: Vec<&str> = additions
            .difference(&self.cleanup)
            .map(AsRef::as_ref)
            .collect();

        if !sql_additions.is_empty() {
            let result =
                tokio::time::timeout(SUBSCRIPTION_TIMEOUT, listener.listen_all(sql_additions))
                    .await
                    .map_err(|_| timed_out())
                    .flatten();

            if let Err(error) = result {
                // Retry on a fresh listener. Cancelled or timed-out requests
                // close their acknowledgement and are skipped on the next pass.
                for command in commands.into_iter().rev() {
                    self.pending.push_front(command);
                }
                return Err(error);
            }
        }

        {
            let mut subscriptions = self.subscriptions.inner.write().await;
            for channel in additions {
                self.cleanup.remove(&channel);
                let (tx, _) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
                subscriptions.insert(channel, ChannelSubscription { ref_count: 0, tx });
            }
        }

        self.dispatch_buffered(listener).await;
        {
            let mut subscriptions = self.subscriptions.inner.write().await;
            for command in commands {
                match command {
                    ListenerCommand::Listen((channel, response)) => {
                        if response.is_closed() {
                            continue;
                        }

                        let subscription = subscriptions
                            .get_mut(&channel)
                            .expect("LISTEN installed before acknowledgement");

                        if response.send(Ok(subscription.tx.subscribe())).is_ok() {
                            subscription.ref_count += 1;
                        }
                    }
                    ListenerCommand::Unlisten((channel, response)) => {
                        if let Some(subscription) = subscriptions.get_mut(&channel) {
                            subscription.ref_count = subscription.ref_count.saturating_sub(1);
                        }
                        // Logical removal is committed exactly once. If SQL
                        // cleanup fails, reconnection restores only live refs.
                        let _ = response.send(Ok(()));
                    }
                }
            }

            // Defer SQL cleanup so new subscriptions can be acknowledged
            // between individual removals in forward().
            self.cleanup.extend(
                subscriptions
                    .extract_if(|_, subscription| subscription.ref_count == 0)
                    .map(|(channel, _)| channel),
            );
        }

        Ok(())
    }
}

fn timed_out() -> sqlx::Error {
    sqlx::Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "notification listener operation timed out",
    ))
}

#[cfg(all(test, feature = "e2e"))]
mod recovery_tests {
    use tokio::io::{
        AsyncReadExt as _,
        AsyncWriteExt as _,
    };
    use tokio::net::{
        TcpListener,
        TcpStream,
    };

    use super::*;
    use crate::SubscriptionEvent;

    async fn pool() -> sqlx::PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:34030/app".to_owned());

        sqlx::PgPool::connect(&database_url).await.unwrap()
    }

    #[tokio::test]
    async fn restoration_discards_notifications_buffered_by_subscription_sql() {
        let pool = pool().await;
        let service = ListenerService::try_new(&pool).await.unwrap();
        let channel = Channel::try_from("buffered_restoration".to_owned()).unwrap();
        let (sender, mut notifications) = broadcast::channel(128);

        service.subscriptions.inner.write().await.insert(
            channel.clone(),
            ChannelSubscription {
                ref_count: 1,
                tx: sender,
            },
        );

        service.lifecycle_tx.send(false).unwrap();
        let mut lifecycle = service.lifecycle_rx.clone();
        lifecycle.borrow_and_update();
        let mut listener = ListenerService::connect(&service.options).await.unwrap();
        listener.listen(channel.as_ref()).await.unwrap();

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.as_ref())
            .bind("obsolete")
            .execute(&mut listener)
            .await
            .unwrap();

        sqlx::query("SELECT 1")
            .execute(&mut listener)
            .await
            .unwrap();

        service.restore_subscriptions(&mut listener).await.unwrap();
        lifecycle.changed().await.unwrap();
        assert!(*lifecycle.borrow_and_update());
        assert!(listener.next_buffered().is_none());

        assert!(matches!(
            notifications.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.as_ref())
            .bind("fresh")
            .execute(&pool)
            .await
            .unwrap();

        let notification = tokio::time::timeout(Duration::from_secs(2), listener.try_recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(notification.payload(), "fresh");
    }

    struct LatencyProxy {
        address: std::net::SocketAddr,
        task: tokio::task::JoinHandle<()>,
        unlisten_delay_ms: Arc<std::sync::atomic::AtomicU64>,
        unlisten_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for LatencyProxy {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn latency_proxy(pool: &sqlx::PgPool, delay: Duration) -> LatencyProxy {
        use std::sync::atomic::{
            AtomicU64,
            AtomicUsize,
            Ordering,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let options = pool.connect_options();
        let upstream = format!("{}:{}", options.get_host(), options.get_port());
        let unlisten_delay_ms = Arc::new(AtomicU64::new(0));
        let unlisten_count = Arc::new(AtomicUsize::new(0));
        let requested_delay = Arc::clone(&unlisten_delay_ms);
        let observed = Arc::clone(&unlisten_count);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (downstream, _) = listener.accept().await.unwrap();
                let upstream = TcpStream::connect(&upstream).await.unwrap();
                let requested_delay = Arc::clone(&requested_delay);
                let observed = Arc::clone(&observed);
                connections.spawn(async move {
                    let pending_delay = AtomicU64::new(0);
                    let (mut read_client, mut write_client) = downstream.into_split();
                    let (mut read_server, mut write_server) = upstream.into_split();

                    let requests = async {
                        let mut buffer = [0; 65536];
                        let mut request_tail = Vec::new();

                        loop {
                            let length = read_client.read(&mut buffer).await?;
                            if length == 0 {
                                break;
                            }
                            // Keep enough overlap to recognize a command split
                            // across TCP reads without counting it twice.
                            request_tail.extend_from_slice(&buffer[..length]);

                            if request_tail.windows(8).any(|part| part == b"UNLISTEN") {
                                observed.fetch_add(1, Ordering::SeqCst);
                                pending_delay.store(
                                    requested_delay.swap(0, Ordering::SeqCst),
                                    Ordering::SeqCst,
                                );
                            }

                            let consumed = request_tail.len().saturating_sub(7);
                            request_tail.drain(..consumed);
                            write_server.write_all(&buffer[..length]).await?;
                        }
                        Ok::<(), std::io::Error>(())
                    };

                    let responses = async {
                        let mut buffer = [0; 65536];
                        loop {
                            let length = read_server.read(&mut buffer).await?;
                            if length == 0 {
                                break;
                            }

                            let delay = delay.max(Duration::from_millis(
                                pending_delay.swap(0, Ordering::SeqCst),
                            ));

                            tokio::time::sleep(delay).await;
                            write_client.write_all(&buffer[..length]).await?;
                        }
                        Ok::<(), std::io::Error>(())
                    };

                    tokio::select! { _ = requests => {}, _ = responses => {} }
                });
            }
        });
        LatencyProxy {
            address,
            task,
            unlisten_delay_ms,
            unlisten_count,
        }
    }

    async fn proxied_service(pool: &sqlx::PgPool, proxy: &LatencyProxy) -> ListenerService {
        let options = (*pool.connect_options())
            .clone()
            .host("127.0.0.1")
            .port(proxy.address.port());
        ListenerService::try_new(&PgPoolOptions::new().connect_lazy_with(options))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn cleanup_progresses_during_sustained_subscription_churn() {
        use std::sync::atomic::{
            AtomicUsize,
            Ordering,
        };
        let pool = pool().await;
        let proxy = latency_proxy(&pool, Duration::from_millis(20)).await;
        let mut service = proxied_service(&pool, &proxy).await;
        let handle = service.notification_listener();
        let commands = service.command_tx.clone();
        let job = tokio::spawn(async move { service.run_connected().await });
        let obsolete = futures::future::join_all((0..16).map(|index| {
            let handle = handle.clone();
            async move {
                handle
                    .listen(Channel::try_from(format!("sustained_obsolete_{index}")).unwrap())
                    .await
                    .unwrap()
            }
        }))
        .await;
        // Hold the first cleanup response until producers have queued work.
        proxy.unlisten_delay_ms.store(500, Ordering::SeqCst);
        drop(obsolete);
        tokio::time::timeout(Duration::from_secs(2), async {
            while proxy.unlisten_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..COMMAND_BATCH_SIZE * 2 {
            let handle = handle.clone();
            let completed = Arc::clone(&completed);
            workers.spawn(async move {
                let mut generation = 0_u64;
                loop {
                    let channel =
                        Channel::try_from(format!("sustained_churn_{worker}_{generation}"))
                            .unwrap();

                    let guard = handle.listen(channel).await.unwrap();
                    completed.fetch_add(1, Ordering::SeqCst);
                    drop(guard);
                    generation += 1;
                }
            });
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let before = proxy.unlisten_count.load(Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(3), async {
            while proxy.unlisten_count.load(Ordering::SeqCst) < before + 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cleanup must progress while producers continue subscribing");

        assert!(
            commands.capacity() < commands.max_capacity(),
            "subscription commands should still be queued when cleanup progresses"
        );

        assert!(!job.is_finished());
        workers.abort_all();
        while workers.join_next().await.is_some() {}
        job.abort();
        let _ = job.await;
    }

    #[tokio::test]
    async fn new_subscribers_are_serviced_between_cleanup_operations() {
        use std::sync::atomic::Ordering;
        let pool = pool().await;
        let proxy = latency_proxy(&pool, Duration::from_millis(300)).await;
        let mut service = proxied_service(&pool, &proxy).await;
        let handle = service.notification_listener();
        let job = tokio::spawn(async move { service.run_connected().await });
        let existing = Channel::try_from("cleanup_fairness_existing".to_owned()).unwrap();
        let mut existing_guard = handle.listen(existing.clone()).await.unwrap();
        let cancelled = futures::future::join_all((0..64).map(|index| {
            let handle = handle.clone();
            async move {
                handle
                    .listen(Channel::try_from(format!("cleanup_fairness_{index}")).unwrap())
                    .await
                    .unwrap()
            }
        }))
        .await;

        proxy.unlisten_delay_ms.store(2200, Ordering::SeqCst);
        let start = tokio::time::Instant::now();
        drop(cancelled);
        tokio::time::timeout(Duration::from_secs(3), async {
            while proxy.unlisten_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let _replacement =
            tokio::time::timeout(Duration::from_secs(15), handle.listen(existing.clone()))
                .await
                .unwrap()
                .unwrap();

        let _new = tokio::time::timeout(
            Duration::from_secs(15),
            handle.listen(Channel::try_from("cleanup_fairness_new".to_owned()).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            proxy.unlisten_count.load(Ordering::SeqCst) < 64,
            "acknowledgements must not wait for the cleanup batch to finish"
        );

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(existing.as_ref())
            .bind("after cleanup")
            .execute(&pool)
            .await
            .unwrap();

        let notification =
            tokio::time::timeout(Duration::from_secs(5), existing_guard.recv_event())
                .await
                .unwrap()
                .unwrap();

        assert!(
            matches!(notification, SubscriptionEvent::Notification(notification) if notification.payload == "after cleanup")
        );

        assert!(start.elapsed() >= Duration::from_millis(2200));
        assert!(
            !job.is_finished(),
            "slow cleanup must not trigger global recovery"
        );

        job.abort();
        let _ = job.await;
    }

    #[tokio::test]
    async fn terminated_connection_restores_listen_before_signaling_ready() {
        let pool = pool().await;
        let mut service = ListenerService::try_new(&pool).await.unwrap();

        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(service.pg_listener.as_mut().unwrap())
            .await
            .unwrap();

        let handle = service.notification_listener();

        let job = tokio::spawn(async move {
            let result = service.run_connected().await;
            service.lifecycle_tx.send(false).unwrap();
            (service, result)
        });

        let channel = Channel::try_from("terminated_listener_recovery".to_owned()).unwrap();
        let mut guard = handle.listen(channel.clone()).await.unwrap();

        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&pool)
            .await
            .unwrap();

        let (mut service, result) = tokio::time::timeout(Duration::from_secs(3), job)
            .await
            .unwrap()
            .unwrap();

        assert!(result.is_err());

        assert!(matches!(
            guard.recv_event().await.unwrap(),
            SubscriptionEvent::Unavailable
        ));

        let job = tokio::spawn(async move { service.run_connected().await });

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), guard.recv_event())
                .await
                .unwrap()
                .unwrap(),
            SubscriptionEvent::Restored
        ));

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.as_ref())
            .bind("after")
            .execute(&pool)
            .await
            .unwrap();

        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(2), guard.recv_event()).await.unwrap().unwrap(), SubscriptionEvent::Notification(notification) if notification.payload == "after")
        );

        job.abort();

        let _ = job.await;
    }

    #[tokio::test]
    async fn failed_subscription_batch_preserves_cancellation_reference_counts() {
        let pool = pool().await;
        let mut service = ListenerService::try_new(&pool).await.unwrap();
        let mut listener = service.pg_listener.take().unwrap();
        let existing = Channel::try_from("existing_failed_batch".to_owned()).unwrap();
        let cancelled = Channel::try_from("cancelled_failed_batch".to_owned()).unwrap();
        listener.listen(existing.as_ref()).await.unwrap();
        let (tx, mut notifications) = broadcast::channel(128);
        service
            .subscriptions
            .inner
            .write()
            .await
            .insert(existing.clone(), ChannelSubscription { ref_count: 1, tx });
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut listener)
            .await
            .unwrap();
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&pool)
            .await
            .unwrap();
        let (first, mut first_response) = oneshot::channel();
        let (second, second_response) = oneshot::channel();
        drop(second_response);
        service
            .command_tx
            .send(ListenerCommand::Listen((existing.clone(), second)))
            .await
            .unwrap();
        assert!(
            service
                .apply_commands(
                    &mut listener,
                    ListenerCommand::Listen((cancelled.clone(), first))
                )
                .await
                .is_err()
        );
        first_response.close();
        assert_eq!(service.pending.len(), 2);
        assert_eq!(service.channel_ref_count(&existing).await, 1);
        drop(listener);
        let handle = service.notification_listener();
        let subscriptions = service.subscriptions();
        let job = tokio::spawn(async move { service.run_connected().await });

        let _new = handle
            .listen(Channel::try_from("valid_after_cancelled_batch".to_owned()).unwrap())
            .await
            .unwrap();

        assert_eq!(subscriptions.channel_ref_count(&existing).await, 1);
        assert_eq!(subscriptions.channel_ref_count(&cancelled).await, 0);
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(existing.as_ref())
            .bind("surviving reference")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), notifications.recv())
                .await
                .unwrap()
                .unwrap()
                .payload,
            "surviving reference"
        );
        job.abort();
        let _ = job.await;
    }

    #[tokio::test]
    async fn half_open_connection_health_timeout_allows_fresh_connection() {
        use std::sync::atomic::{
            AtomicBool,
            Ordering,
        };
        let pool = pool().await;
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = proxy_listener.local_addr().unwrap();
        let options = pool.connect_options();
        let upstream = format!("{}:{}", options.get_host(), options.get_port());
        let blackhole = Arc::new(AtomicBool::new(false));
        let proxy_blackhole = Arc::clone(&blackhole);
        let proxy = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut first = true;
            loop {
                let (downstream, _) = proxy_listener.accept().await.unwrap();
                let upstream = TcpStream::connect(&upstream).await.unwrap();

                let block_this_connection = first;
                first = false;

                let blackhole = Arc::clone(&proxy_blackhole);
                connections.spawn(async move {
                    let (mut downstream_read, mut downstream_write) = downstream.into_split();
                    let (mut upstream_read, mut upstream_write) = upstream.into_split();
                    let responses = async {
                        let mut buffer = [0; 65536];
                        loop {
                            let length = upstream_read.read(&mut buffer).await?;
                            if length == 0 {
                                break;
                            }
                            if !block_this_connection || !blackhole.load(Ordering::SeqCst) {
                                downstream_write.write_all(&buffer[..length]).await?;
                            }
                        }

                        Ok::<(), std::io::Error>(())
                    };

                    tokio::select! {
                        _ = tokio::io::copy(&mut downstream_read, &mut upstream_write) => {},
                        _ = responses => {},
                    }
                });
            }
        });

        let proxied_pool = PgPoolOptions::new()
            .connect_lazy_with((*options).clone().host("127.0.0.1").port(address.port()));

        let mut service = ListenerService::try_new(&proxied_pool).await.unwrap();
        let handle = service.notification_listener();

        let job = tokio::spawn(async move {
            let result = service.run_connected().await;
            service.lifecycle_tx.send(false).unwrap();
            (service, result)
        });

        let channel = Channel::try_from("half_open_listener_recovery".to_owned()).unwrap();

        let mut guard = handle.listen(channel.clone()).await.unwrap();
        blackhole.store(true, Ordering::SeqCst);

        let (mut service, result) = tokio::time::timeout(Duration::from_secs(19), job)
            .await
            .unwrap()
            .unwrap();

        assert!(result.is_err());

        assert!(matches!(
            guard.recv_event().await.unwrap(),
            SubscriptionEvent::Unavailable
        ));

        let job = tokio::spawn(async move { service.run_connected().await });

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), guard.recv_event())
                .await
                .unwrap()
                .unwrap(),
            SubscriptionEvent::Restored
        ));

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.as_ref())
            .bind("restored")
            .execute(&pool)
            .await
            .unwrap();

        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(2), guard.recv_event()).await.unwrap().unwrap(), SubscriptionEvent::Notification(notification) if notification.payload == "restored")
        );

        job.abort();

        let _ = job.await;
        proxy.abort();

        let _ = proxy.await;
    }
}
