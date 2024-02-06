use async_trait::async_trait;
use crate::bin_types::{BinKey, BinValue};
use crate::table::table::{Table, TableEngine};

use crate::disk_storage::storage::DiskStorage;
use crate::index::Index;
use crate::scheme::scheme::Scheme;
use crate::writers::LogWriter;

pub struct OnDiskTable<I: Index<BinKey, (u64, u64)>> {
    core: DiskStorage<I>,
    name: String,
    scheme: Scheme,
    user_scheme: Box<[u8]>,
}

impl<I: Index<BinKey, (u64, u64)>> OnDiskTable<I> {
    pub(crate) fn new(
        name: String,
        size: usize,
        index: I,
        scheme: Scheme,
        user_scheme: Box<[u8]>,
    ) -> OnDiskTable<I> {
        OnDiskTable {
            core: DiskStorage::new(name.clone(), size, index),
            name,
            scheme,
            user_scheme,
        }
    }
}

#[async_trait]
impl<I: Index<BinKey, (u64, u64)>> Table for OnDiskTable<I> {
    #[inline(always)]
    fn engine(&self) -> TableEngine {
        TableEngine::OnDisk
    }

    #[inline(always)]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[inline(always)]
    fn is_it_logging(&self) -> bool {
        unreachable!();
    }

    #[inline(always)]
    fn cache_duration(&self) -> u64 {
        unreachable!()
    }

    #[inline(always)]
    fn get(&mut self, key: &BinKey) -> Option<BinValue> {
        self.core.get(key)
    }

    #[inline(always)]
    async fn set(&mut self, key: BinKey, value: BinValue, _: &mut LogWriter) -> Option<BinValue> {
        self.core.set(key, value)
    }

    #[inline(always)]
    fn set_without_log(&mut self, key: BinKey, value: BinValue) -> Option<BinValue> {
        self.core.set(key, value)
    }

    #[inline(always)]
    async fn insert(&mut self, key: BinKey, value: BinValue, _: &mut LogWriter) -> bool {
        self.core.insert(key, value)
    }

    #[inline(always)]
    fn insert_without_log(&mut self, key: BinKey, value: BinValue) -> bool {
        self.core.insert(key, value)
    }

    #[inline(always)]
    async fn delete(&mut self, key: &BinKey, _: &mut LogWriter) {
        self.core.delete(key);
    }

    #[inline(always)]
    fn delete_without_log(&mut self, key: &BinKey) {
        self.core.delete(key);
    }

    fn count(&self) -> u64 {
        self.core.infos.count() as u64
    }

    fn user_scheme(&self) -> Box<[u8]> {
        self.user_scheme.clone()
    }

    fn scheme(&self) -> &Scheme {
        &self.scheme
    }

    fn rise(&mut self) {
        self.core.rise();
    }
    
    // NOT EXISTS!

    fn invalid_cache(&mut self) {
        unreachable!()
    }

    fn dump(&mut self) {
        return;
    }
}

unsafe impl<I: Index<BinKey, (u64, u64)>> Send for OnDiskTable<I> {}
unsafe impl<I: Index<BinKey, (u64, u64)>> Sync for OnDiskTable<I> {}