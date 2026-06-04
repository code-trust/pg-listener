use sqlx::postgres::PgNotification;

use crate::Channel;

#[derive(Clone, Debug)]
pub struct Notification {
    pub process_id: u32,
    pub channel: Channel,
    pub payload: String,
}

impl From<PgNotification> for Notification {
    fn from(notif: PgNotification) -> Self {
        let channel = notif.channel();
        Self {
            process_id: notif.process_id(),
            channel: Channel::try_from(channel.to_owned()).unwrap_or_else(|_| {
                panic!("invalid channel name received from postgres: {channel}")
            }),
            payload: notif.payload().to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct TypedNotification<T: std::fmt::Debug> {
    pub process_id: u32,
    pub channel: Channel,
    pub payload: T,
}
