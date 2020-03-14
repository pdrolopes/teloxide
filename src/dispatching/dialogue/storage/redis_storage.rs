use super::Storage;
use futures::future::BoxFuture;
use redis::{AsyncCommands, IntoConnectionInfo};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use thiserror::Error;
use super::serializer::{self, Serializer};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    SerdeError(#[from] serializer::Error),
    #[error("error from Redis: {0}")]
    RedisError(#[from] redis::RedisError),
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct RedisStorage {
    client: redis::Client,
    serializer: Serializer,
}

impl RedisStorage {
    pub fn open(
        url: impl IntoConnectionInfo,
        serializer: Serializer,
    ) -> Result<Self> {
        Ok(Self { client: redis::Client::open(url)?, serializer })
    }
}

impl<D> Storage<D> for RedisStorage
where
    D: Send + Serialize + DeserializeOwned + 'static,
{
    type Error = Error;

    // `.del().ignore()` is much more readable than `.del()\n.ignore()`
    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn remove_dialogue(
        self: Arc<Self>,
        chat_id: i64,
    ) -> BoxFuture<'static, Result<Option<D>>> {
        Box::pin(async move {
            let mut conn = self.client.get_async_connection().await?;
            Ok(redis::pipe()
                .atomic()
                .get(chat_id)
                .del(chat_id).ignore()
                .query_async::<_, Option<Vec<u8>>>(&mut conn)
                .await?
                .map(|d| self.serializer.deserialize(&d))
                .transpose()?)
        })
    }

    fn update_dialogue(
        self: Arc<Self>,
        chat_id: i64,
        dialogue: D,
    ) -> BoxFuture<'static, Result<Option<D>>> {
        Box::pin(async move {
            let mut conn = self.client.get_async_connection().await?;
            let dialogue = self.serializer.serialize(&dialogue)?;
            Ok(conn
                .getset::<_, Vec<u8>, Option<Vec<u8>>>(chat_id, dialogue)
                .await?
                .map(|d| self.serializer.deserialize(&d))
                .transpose()?)
        })
    }
}
