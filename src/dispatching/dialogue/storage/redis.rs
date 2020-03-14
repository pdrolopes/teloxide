#![allow(dead_code)]
#![allow(unused_imports)]

use super::Storage;
use futures::future::BoxFuture;
use std::sync::Arc;
use redis::{AsyncCommands, IntoConnectionInfo};
use thiserror::Error;
use serde::{Serialize, de::DeserializeOwned};
pub use Serializer::*;


#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed parsing/serializing JSON: {0}")]
    JSONError(#[from] serde_json::Error),
    #[error("Error from Redis: {0}")]
    RedisError(#[from] redis::RedisError),
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub enum Serializer {
    JSON,
}

impl Serializer {
    pub fn serialize<D>(&self, val: &D) -> Result<Vec<u8>>
    where D: Serialize {
        Ok(match self {
            JSON => serde_json::to_vec(val)?
        })
    }

    pub fn deserialize<'de, D>(&self, data: &'de [u8]) -> Result<D>
    where D: DeserializeOwned {
        Ok(match self {
            JSON => serde_json::from_slice(data)?
        })
    }
}

pub struct RedisStorage {
    client: redis::Client,
    serializer: Serializer
}

impl RedisStorage {
    pub fn open(url: impl IntoConnectionInfo, serializer: Serializer) -> Result<Self> {
        Ok(Self {
            client: redis::Client::open(url)?,
            serializer,
        })
    }
}

impl<D> Storage<D> for RedisStorage 
where D: Send + Serialize + DeserializeOwned + 'static {
    type Error = Error;

    fn remove_dialogue(
        self: Arc<Self>,
        chat_id: i64
    ) -> BoxFuture<'static, Result<Option<D>>> {
        Box::pin(
            async move {
                let mut conn = self.client.get_async_connection().await?;
                let old_val: Option<D> = conn
                    .get::<_, Option<Vec<u8>>>(chat_id)
                    .await?
                    .map(|d| self.serializer.deserialize(&d))
                    .transpose()?;
                conn.del(chat_id).await?;
                Ok(old_val)
            }
        )
    }

    fn update_dialogue(
        self: Arc<Self>,
        chat_id: i64,
        dialogue: D
    ) -> BoxFuture<'static, Result<Option<D>>> {
        Box::pin(
            async move {
                let mut conn = self.client.get_async_connection().await?;
                let dialogue = self.serializer.serialize(&dialogue)?;
                Ok(
                    conn
                        .getset::<_, Vec<u8>, Option<Vec<u8>>>(
                            chat_id,
                            dialogue
                        )
                        .await?
                        .map(|d| self.serializer.deserialize(&d))
                        .transpose()?
                )
            }
        )
    }
}
