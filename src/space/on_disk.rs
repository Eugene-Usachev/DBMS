use crate::space::space::Space;

use crate::disk_storage::storage::DiskStorage;

pub struct OnDiskSpace {
    core: DiskStorage,
}

impl OnDiskSpace {
    pub(crate) fn new(name: String, size: usize) -> Self {
        OnDiskSpace {
            core: DiskStorage::new(name, size),
        }
    }
}

impl Space for OnDiskSpace {
    #[inline(always)]
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.core.get(key)
    }

    #[inline(always)]
    fn set(&self, key: Vec<u8>, value: Vec<u8>, _log_buffer: &mut [u8], _log_buffer_offset: &mut usize) {
        self.core.set(key, value);
    }

    #[inline(always)]
    fn insert(&self, key: Vec<u8>, value: Vec<u8>, _log_buffer: &mut [u8], _log_buffer_offset: &mut usize) {
        self.core.insert(key, value);
    }

    #[inline(always)]
    fn delete(&self, key: Vec<u8>, _log_buffer: &mut [u8], _log_buffer_offset: &mut usize) {
        self.core.delete(&key);
    }

    fn count(&self) -> u64 {
        let mut count = 0;
        for _ in self.core.infos.iter() {
            count += 1;
        }
        return count;
    }

    fn rise(&mut self) {
        self.core.rise();
    }
    
    // NOT EXISTS!

    fn invalid_cache(&self) {
        unreachable!()
    }

    fn get_and_reset_cache_time(&self, _key: &[u8]) -> Option<Vec<u8>> {
        unreachable!()
    }

    fn dump(&self) {
        return;
    }
}