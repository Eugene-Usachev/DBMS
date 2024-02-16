use std::error::Error;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::connection::{BufConnection, Status, SyncBufConnection};
use crate::constants::actions;
use crate::error::CustomError;
use crate::shard::{reactions};
use crate::storage::Storage;

pub type ShardRequest = SyncBufConnection;

/// ShardResponse contains is_ok
pub type ShardResponse = (SyncBufConnection, bool);

pub struct Shard {
    pub storage: Storage
}

impl<'a> Shard {
    pub async fn create(number: usize) -> Shard {
        let shard = Shard {
            storage: Storage::new(number).await,
        };
        return shard;
    }

    pub fn run(mut shard: &mut Shard, sender: Sender<ShardResponse>, mut receiver: Receiver<ShardRequest>) {
        tokio::runtime::Builder::new_current_thread().thread_name("shard worker").enable_all().build().unwrap().block_on(async move {
            let mut conn;
            loop {
                conn = receiver.recv().await.unwrap();
                let res = shard.handle_connection(&mut conn).await;
                if res.is_err() {
                    sender.send((conn, false)).await.unwrap();
                    continue;
                }
                sender.send((conn, true)).await.unwrap();
            }
        });
    }

    pub async fn handle_connection(&mut self, mut connection: &mut ShardRequest) -> Result<(), Box<dyn Error>> {
        let mut message;
        let mut status;
        loop {
            (message, status) = connection.read_message().await;
            if status != Status::Ok {
                if status == Status::All {
                    self.storage.log_writer.flush().await;
                    break;
                }
                connection.close().await.expect("Failed to close connection");
                return Err(Box::new(CustomError::new("Bad request")));
            }

            status = self.handle_message(connection.get(), message).await;
            if status != Status::Ok {
                connection.close().await.expect("Failed to close connection");
                return Err(Box::new(CustomError::new("Bad request")));
            }
        }

        return Ok(());
    }

    #[inline(always)]
    async fn handle_message(&mut self, connection: &mut BufConnection, message: &[u8]) -> Status {
        return match message[0] {
            actions::GET => reactions::work_with_tables::get(connection, &mut self.storage, message).await,
            actions::GET_FIELD => reactions::work_with_tables::get_field(connection, &mut self.storage, message).await,
            actions::GET_FIELDS => reactions::work_with_tables::get_fields(connection, &mut self.storage, message).await,

            actions::INSERT => reactions::work_with_tables::insert(connection, &mut self.storage, message).await,
            actions::SET => reactions::work_with_tables::set(connection, &mut self.storage, message).await,
            actions::DELETE => reactions::work_with_tables::delete(connection, &mut self.storage, message).await,
            _ => {
                connection.write_message(&[actions::BAD_REQUEST]).await
            }
        }
    }

}