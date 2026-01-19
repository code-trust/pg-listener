use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use backend::configuration::{
    ConfigurationDirectory,
    get_configuration,
};
use backend::listener::{
    Channel,
    ListenerService,
    NotificationListener,
    TypedChannel,
    TypedRecvError,
};
use secrecy::ExposeSecret as _;
use serde::{
    Deserialize,
    Serialize,
};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio_graceful_shutdown::Toplevel;

#[tokio::test]
async fn publish_and_receive_simple_message() {
    let (listener_service, pool) = create_listener_service().await;

    let listener = listener_service.notification_listener();
    let channel = simple_channel("publish_and_receive_simple_message");

    let mut channel_guard = listener
        .listen_typed(channel.clone())
        .await
        .expect("Failed to listen to channel");

    let publish_channel = channel.clone();
    tokio::spawn(async move {
        publish_channel
            .publish_pool(&pool, &"This is just a test".to_owned())
            .await
            .expect("Failed to publish notification");
    });

    let notification = channel_guard
        .recv()
        .await
        .expect("Failed to get notification");
    assert_eq!(notification.channel, channel.into());
    assert_eq!(notification.payload, "This is just a test");
}

#[tokio::test]
async fn publish_and_receive_complex_message() {
    let (listener_service, pool) = create_listener_service().await;
    let listener = listener_service.notification_listener();
    let channel = complex_channel();
    let mut channel_guard = listener
        .listen_typed(channel.clone())
        .await
        .expect("Failed to listen to channel");
    let publish_channel = channel.clone();
    tokio::spawn(async move {
        publish_channel
            .publish_pool(
                &pool,
                &ComplexMessage {
                    message: "This is just a test".to_owned(),
                },
            )
            .await
            .expect("Failed to publish notification");
    });
    let notification = channel_guard
        .recv()
        .await
        .expect("Failed to get notification");
    assert_eq!(notification.channel, channel.into());
    assert_eq!(
        notification.payload,
        ComplexMessage {
            message: "This is just a test".to_owned()
        }
    );
}

#[tokio::test]
async fn supports_multiple_listeners() {
    let (listener_service, pool) = create_listener_service().await;
    let listener = listener_service.notification_listener();
    let channel = simple_channel("supports_multiple_listeners");
    let mut channel_guard_one = listener
        .listen_typed(channel.clone())
        .await
        .expect("Failed to listen to channel");
    let mut channel_guard_two = listener
        .listen_typed(channel.clone())
        .await
        .expect("Failed to listen to channel");
    let publish_channel = channel.clone();
    tokio::spawn(async move {
        publish_channel
            .publish_pool(&pool, &"This is just a test".to_owned())
            .await
            .expect("Failed to publish notification");
    });
    let notification = channel_guard_one
        .recv()
        .await
        .expect("Failed to get notification");
    assert_eq!(notification.channel, channel.clone().into());
    assert_eq!(notification.payload, "This is just a test");

    let notification = channel_guard_two
        .recv()
        .await
        .expect("Failed to get notification");
    assert_eq!(notification.channel, channel.into());
    assert_eq!(notification.payload, "This is just a test");
}

#[tokio::test]
async fn channel_guard_removes_listener_when_dropped() {
    let (listener_service, _pool) = create_listener_service().await;
    let listener = listener_service.notification_listener();
    let channel = simple_channel("channel_guard_removes_listener_when_dropped");
    {
        let _channel_guard = listener
            .listen_typed(channel.clone())
            .await
            .expect("Failed to listen to channel");
        assert_eq!(
            listener_service.channel_ref_count(channel.as_ref()).await,
            1
        );

        {
            let _channel_guard_2 = listener
                .listen_typed(channel.clone())
                .await
                .expect("Failed to listen to channel");
            assert_eq!(
                listener_service.channel_ref_count(channel.as_ref()).await,
                2
            );
        }

        wait_until(
            || async { listener_service.channel_ref_count(channel.as_ref()).await == 1 },
            Duration::from_millis(500),
        )
        .await;
    }

    wait_until(
        || async { listener_service.channel_ref_count(channel.as_ref()).await == 0 },
        Duration::from_millis(500),
    )
    .await;
}

#[tokio::test]
async fn handles_invalid_json_gracefully() {
    let (listener_service, pool) = create_listener_service().await;
    let listener = listener_service.notification_listener();
    let channel: TypedChannel<String> = TypedChannel::try_from("invalid_json".to_owned()).unwrap();
    let mut guard = listener.listen_typed(channel.clone()).await.unwrap();

    let publish_channel = channel.clone();
    tokio::spawn(async move {
        sqlx::query!(
            "SELECT pg_notify($1, $2)",
            publish_channel.as_ref().as_ref(),
            "invalid json"
        )
        .execute(&pool)
        .await
        .unwrap();
    });

    let result = guard.recv().await;
    assert!(matches!(result, Err(TypedRecvError::Deserialize { .. })));
}

#[tokio::test]
async fn cleanup_when_guard_dropped_during_receive() {
    let (listener_service, pool) = create_listener_service().await;
    let listener = listener_service.notification_listener();
    let channel = simple_channel("cleanup_when_guard_dropped_during_receive");

    {
        let mut guard = listener.listen_typed(channel.clone()).await.unwrap();
        // Start receiving but drop before message arrives
        tokio::spawn(async move {
            let _ = guard.recv().await;
        });
    }

    let publish_channel = channel.clone();
    tokio::spawn(async move {
        publish_channel
            .publish_pool(&pool, &"test".to_owned())
            .await
            .unwrap();
    });

    wait_until(
        || async { listener_service.channel_ref_count(channel.as_ref()).await == 0 },
        Duration::from_millis(500),
    )
    .await;
}

#[tokio::test]
async fn handles_rapid_subscribe_unsubscribe() {
    let (listener_service, _pool) = create_listener_service().await;
    let listener = listener_service.notification_listener();
    let channel = simple_channel("handles_rapid_subscribe_unsubscribe");

    for _ in 0..50 {
        let _guard = listener.listen_typed(channel.clone()).await.unwrap();
    }

    wait_until(
        || async { listener_service.channel_ref_count(channel.as_ref()).await == 0 },
        Duration::from_millis(500),
    )
    .await;
}

fn simple_channel(channel: &str) -> TypedChannel<String> {
    TypedChannel::try_from(channel.to_owned()).unwrap()
}

fn complex_channel() -> TypedChannel<ComplexMessage> {
    TypedChannel::try_from("complex".to_owned()).unwrap()
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
struct ComplexMessage {
    pub message: String,
}

struct TestListenerService {
    listener: NotificationListener,
    channel_refs: Arc<RwLock<HashMap<Channel, usize>>>,
}
impl TestListenerService {
    pub fn notification_listener(&self) -> NotificationListener {
        self.listener.clone()
    }

    pub async fn channel_ref_count(&self, channel: &Channel) -> usize {
        let refs = self.channel_refs.read().await;
        *refs.get(channel).unwrap_or(&0)
    }
}

async fn create_listener_service() -> (TestListenerService, PgPool) {
    let mut configuration =
        get_configuration(ConfigurationDirectory::default()).expect("Failed to get configuration");
    configuration.database.url = "postgres://{user}:{pass}@localhost:34006/app".to_owned();

    let tenant_url = configuration.database.tenant_url();
    let url = tenant_url.expose_secret();
    let pool = PgPool::connect(url)
        .await
        .expect("Failed to connect to database");

    let listener_service = ListenerService::try_new(&pool)
        .await
        .expect("Failed to create listener service");

    let listener = listener_service.notification_listener();
    let channel_refs = listener_service.channel_refs();

    tokio::spawn(async move {
        Toplevel::new(async |subsys: &mut _| {
            listener_service.start(subsys);
        })
        .handle_shutdown_requests(Duration::from_secs(1))
        .await
        .ok();
    });

    (
        TestListenerService {
            listener,
            channel_refs,
        },
        pool,
    )
}

async fn wait_until<F, Fut>(mut condition: F, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let check_interval = Duration::from_millis(10);
    let start = std::time::Instant::now();

    loop {
        if condition().await {
            return;
        }

        if start.elapsed() >= timeout {
            panic!("Condition was not met within {timeout:?}");
        }

        tokio::time::sleep(check_interval).await;
    }
}
