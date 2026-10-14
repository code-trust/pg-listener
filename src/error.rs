use crate::Channel;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidChannelError {
    #[error("channel length must be 1-63 bytes, got {length}")]
    Length { length: usize },
    #[error("channel must not contain NUL bytes")]
    ContainsNul,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("failed to serialize message")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to publish notifications")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ListenerError {
    #[error("failed to connect listener")]
    Connect(#[source] sqlx::Error),
    #[error("failed to listen to channel {channel}")]
    Listen {
        channel: Channel,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to unlisten from channel {channel}")]
    Unlisten {
        channel: Channel,
        #[source]
        source: sqlx::Error,
    },
    #[error("failed to receive notification")]
    Receive(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ListenError {
    #[error("failed to send listen request: listener service is not running")]
    ServiceClosed,
    #[error("listen service unavailable")]
    ServiceUnavailable,
    #[error("failed to listen to channel {channel}")]
    ListenFailed {
        channel: Channel,
        #[source]
        source: sqlx::Error,
    },
}
