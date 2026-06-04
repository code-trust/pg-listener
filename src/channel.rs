use std::marker::PhantomData;

use derive_more::{
    AsRef,
    Display,
};
use serde::Serialize;
use sqlx::{
    PgConnection,
    PgPool,
};

use crate::error::{
    InvalidChannelLengthError,
    PublishError,
};

// Channel max length 63
#[derive(Debug, Clone, PartialEq, Eq, Hash, AsRef, Display)]
#[as_ref(str)]
pub struct Channel(String);

impl TryFrom<String> for Channel {
    type Error = InvalidChannelLengthError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let length = value.len();
        if !(1..=63).contains(&length) {
            return Err(InvalidChannelLengthError { length });
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, AsRef)]
pub struct TypedChannel<T> {
    #[as_ref]
    inner: Channel,
    _phantom: PhantomData<T>,
}

impl<T> TryFrom<String> for TypedChannel<T> {
    type Error = InvalidChannelLengthError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: Channel::try_from(value)?,
            _phantom: PhantomData,
        })
    }
}

impl<T: Sync> TypedChannel<T> {
    pub async fn publish(&self, conn: &mut PgConnection, message: &T) -> Result<(), PublishError>
    where
        T: Serialize,
    {
        publish_batch(conn, &[(self, message)]).await
    }

    pub async fn publish_pool(&self, pool: &PgPool, message: &T) -> Result<(), PublishError>
    where
        T: Serialize,
    {
        let mut conn = pool.acquire().await?;
        self.publish(&mut conn, message).await
    }
}

pub async fn publish_batch<C, T>(
    conn: &mut PgConnection,
    messages: &[(C, &T)],
) -> Result<(), PublishError>
where
    C: AsRef<Channel> + Sync,
    T: Serialize + Sync,
{
    if messages.is_empty() {
        return Ok(());
    }

    let (channels, payloads): (Vec<_>, Vec<_>) = messages
        .iter()
        .map(|(channel, message)| {
            Ok((
                channel.as_ref().as_ref().to_owned(),
                serde_json::to_string(message)?,
            ))
        })
        .collect::<Result<Vec<_>, PublishError>>()?
        .into_iter()
        .unzip();

    sqlx::query(
        r"
        SELECT pg_notify(input.channel, input.payload)
        FROM unnest($1::text[], $2::text[]) AS input (channel, payload)
        ",
    )
    .bind(&channels)
    .bind(&payloads)
    .execute(conn)
    .await?;

    Ok(())
}

impl<T> From<TypedChannel<T>> for Channel {
    fn from(channel: TypedChannel<T>) -> Self {
        channel.inner
    }
}
