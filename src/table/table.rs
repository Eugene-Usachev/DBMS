use crate::{
    bin_types::{BinKey, BinValue},
    scheme::scheme::{get_field, get_fields, Scheme},
    writers::LogWriter
};

pub trait Table: Sync + Send {
    fn engine(&self) -> TableEngine;
    fn name(&self) -> String;
    fn is_it_logging(&self) -> bool;
    fn cache_duration(&self) -> u64;

    fn get(&self, key: &BinKey) -> Option<BinValue>;

    #[inline(always)]
    fn get_field(&self, key: &BinKey, field: usize) -> Option<Vec<u8>> {
        let res = self.get(key);
        if res.is_none() {
            return None;
        }
        let res = res.unwrap();
        Some(get_field(&res, self.scheme(), field))
    }

    #[inline(always)]
    fn get_fields(&self, key: &BinKey, fields: &[usize]) -> Option<Vec<u8>> {
        let res = self.get(key);
        if res.is_none() {
            return None;
        }
        let res = res.unwrap();
        Some(get_fields(&res, self.scheme(), fields))
    }



    fn set(&self, key: BinKey, value: BinValue,  log_writer: &mut LogWriter) -> Option<BinValue>;
    fn set_without_log(&self, key: BinKey, value: BinValue) -> Option<BinValue>;
    /// Inserts a key-value pair into the index. Do nothing if the key already exists.
    ///
    /// Returns `true` if inserted, `false` otherwise.
    fn insert(&self, key: BinKey, value: BinValue,  log_writer: &mut LogWriter) -> bool;
    fn insert_without_log(&self, key: BinKey, value: BinValue) -> bool;
    fn delete(&self, key: &BinKey,  log_writer: &mut LogWriter);
    fn delete_without_log(&self, key: &BinKey);
    fn count(&self) -> u64;

    /// user_scheme is a scheme, that we get from user. We will not send `scheme::Scheme` to user.
    fn user_scheme(&self) -> Box<[u8]>;
    fn scheme(&self) -> &Scheme;
    fn dump(&self);
    fn rise(&mut self);
    fn invalid_cache(&self);
}

#[repr(u8)]
pub enum TableEngine {
    InMemory = 0,
    OnDisk = 1,
    CACHE = 2
}