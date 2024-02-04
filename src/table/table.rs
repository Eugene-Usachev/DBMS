use crate::bin_types::{BinKey, BinValue};
use crate::scheme::scheme::{get_field, get_fields, Scheme};
use crate::writers::LogWriter;

pub trait Table: Sync + Send {
    fn engine(&self) -> TableEngine;
    fn name(&self) -> String;
    fn is_it_logging(&self) -> bool;
    fn cache_duration(&self) -> u64;

    fn get(&mut self, key: &BinKey) -> Option<BinValue>;

    #[inline(always)]
    fn get_field(&mut self, key: &BinKey, field: usize) -> Option<Vec<u8>> {
        let res = self.get(key);
        if res.is_none() {
            return None;
        }
        let res = res.unwrap();
        Some(get_field(&res, self.scheme(), field))
    }

    #[inline(always)]
    fn get_fields(&mut self, key: &BinKey, fields: &[usize]) -> Option<Vec<u8>> {
        let res = self.get(key);
        if res.is_none() {
            return None;
        }
        let res = res.unwrap();
        Some(get_fields(&res, self.scheme(), fields))
    }

    fn set(&mut self, key: BinKey, value: BinValue,  log_writer: &mut LogWriter) -> Option<BinValue>;
    fn set_without_log(&mut self, key: BinKey, value: BinValue) -> Option<BinValue>;
    /// Inserts a key-value pair into the index. Do nothing if the key already exists.
    ///
    /// Returns `true` if inserted, `false` otherwise.
    fn insert(&mut self, key: BinKey, value: BinValue,  log_writer: &mut LogWriter) -> bool;
    fn insert_without_log(&mut self, key: BinKey, value: BinValue) -> bool;
    fn delete(&mut self, key: &BinKey,  log_writer: &mut LogWriter);
    fn delete_without_log(&mut self, key: &BinKey);
    fn count(&self) -> u64;

    /// user_scheme is a scheme, that we get from user. We will not send `scheme::Scheme` to user.
    fn user_scheme(&self) -> Box<[u8]>;
    fn scheme(&self) -> &Scheme;
    fn dump(&mut self);
    fn rise(&mut self);
    fn invalid_cache(&mut self);
}

#[repr(u8)]
pub enum TableEngine {
    InMemory = 0,
    OnDisk = 1,
    CACHE = 2
}