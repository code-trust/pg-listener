use anyhow::Context as _;
use anyhow::{Result, ensure};
use derive_more::{AsRef, Display};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use std::marker::PhantomData;

// Channel max length 63
#[derive(Debug, Clone, PartialEq, Eq, Hash, AsRef, Display)]
#[as_ref(str)]
pub struct Channel(String);

impl TryFrom<String> for Channel {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ensure!(
            (1..=63).contains(&value.len()),
            "Channel length must be 1-63 characters, got {}",
            value.len()
        );
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
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: Channel::try_from(value)?,
            _phantom: PhantomData,
        })
    }
}

#[cfg_attr(not(feature = "sqlx"), stub_macros::methods)]
impl<T> TypedChannel<T> {
    pub async fn publish(&self, conn: &mut PgConnection, message: &T) -> Result<()>
    where
        T: Serialize,
    {
        let payload = serde_json::to_string(message).context("Failed to serialize message")?;

        sqlx::query!("SELECT pg_notify($1, $2)", self.as_ref().as_ref(), &payload)
            .execute(conn)
            .await
            .context("Failed to publish notification")?;

        Ok(())
    }

    pub async fn publish_pool(&self, pool: &PgPool, message: &T) -> Result<()>
    where
        T: Serialize,
    {
        let mut conn = pool.acquire().await?;
        self.publish(&mut conn, message).await
    }
}

impl<T> From<TypedChannel<T>> for Channel {
    fn from(channel: TypedChannel<T>) -> Self {
        channel.inner
    }
}
