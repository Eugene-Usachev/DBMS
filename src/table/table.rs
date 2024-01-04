use crate::bin_types::{BinKey, BinValue};

pub trait Table: Sync + Send {
    fn get(&self, key: &BinKey) -> Option<BinValue>;
    fn get_and_reset_cache_time(&self, key: &BinKey) -> Option<BinValue>;
    fn set(&self, key: BinKey, value: BinValue, log_buf: &mut [u8], log_offset: &mut usize) -> Option<BinValue>;
    fn set_without_log(&self, key: BinKey, value: BinValue) -> Option<BinValue>;
    /// Inserts a key-value pair into the index. Do nothing if the key already exists.
    ///
    /// Returns `true` if inserted, `false` otherwise.
    fn insert(&self, key: BinKey, value: BinValue, log_buf: &mut [u8], log_offset: &mut usize) -> bool;
    fn insert_without_log(&self, key: BinKey, value: BinValue) -> bool;
    fn delete(&self, key: &BinKey, log_buf: &mut [u8], log_offset: &mut usize);
    fn delete_without_log(&self, key: &BinKey);
    fn count(&self) -> u64;

    fn dump(&self);
    fn rise(&mut self, number_of_dumps: u32);
    fn invalid_cache(&self);
}

pub type SpaceEngineType = u8;
pub const CACHE: SpaceEngineType = 0;
pub const IN_MEMORY: SpaceEngineType = 1;
pub const ON_DISK: SpaceEngineType = 2;