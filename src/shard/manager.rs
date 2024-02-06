use std::sync::{Arc};
use std::sync::atomic::AtomicU32;
use std::{thread};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use crate::shard::shard::{ShardRequest, ShardResponse};
use crate::shard::shard::Shard;
use crate::shard::shard_ref::{ShardRef, Shards};

pub struct Manager {
    pub shards: Shards,
    pub connectors: Box<[(tokio::sync::Mutex<Sender<ShardRequest>>, tokio::sync::Mutex<Receiver<ShardResponse>>)]>,
    pub number_of_dumps: Arc<AtomicU32>,
    pub tables_names: RwLock<Vec<String>>,
}

impl Manager {
    pub async fn new() -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let mut shards = Vec::with_capacity(core_ids.len());
        let mut connectors = Vec::with_capacity(core_ids.len());
        for (i, id) in core_ids.iter().enumerate() {
            let (request_sender, request_receiver) = tokio::sync::mpsc::channel(100);
            let (response_sender, response_receiver) = tokio::sync::mpsc::channel(100);
            connectors.push((Mutex::new(request_sender), Mutex::new(response_receiver)));
            let id = id.clone();
            let shard = Shard::create(i).await;
            let shard_ptr = Box::into_raw(Box::new(shard));
            shards.push(ShardRef::new(shard_ptr));
            let shard_ref = unsafe { &mut *shard_ptr };
            thread::spawn(move || {
                let _res = core_affinity::set_for_current(id);
                Shard::run(shard_ref, response_sender, request_receiver);
            });
        }

        Self {
            shards: Shards {
                shards: shards.into_boxed_slice(),
            },
            connectors: connectors.into_boxed_slice(),
            number_of_dumps: Arc::new(AtomicU32::new(1)),
            tables_names: RwLock::new(Vec::with_capacity(1)),
        }
    }
}

unsafe impl Send for Manager {}