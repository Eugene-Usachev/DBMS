use crate::shard::shard::Shard;
use crate::storage::Storage;

#[derive(Clone, Debug)]
pub struct ShardRef {
    pub shard: *mut Shard,
}

impl ShardRef {
    pub fn new(shard: *mut Shard) -> Self {
        Self {
            shard
        }
    }

    pub fn get_storage(&self) -> &mut Storage {
        unsafe { &mut (*self.shard).storage }
    }
}

unsafe impl Send for ShardRef {}
unsafe impl Sync for ShardRef {}

pub struct Shards {
    pub(crate) shards: Box<[ShardRef]>,
}

unsafe impl Send for Shards {}
unsafe impl Sync for Shards {}