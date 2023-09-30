use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::RwLock;
use ahash::AHashMap;
use crate::space::space::SpaceInterface;
use crate::storage::storage::NOW_MINUTES;
use crate::utils::hash::get_hash::get_hash;

pub struct CacheSpace {
    pub data: Box<[RwLock<AHashMap<Vec<u8>, (u64, Vec<u8>)>>]>,
    pub size: usize,
    cache_duration: u64,
    number_of_dumps: AtomicU32,
    name: String,
}

impl CacheSpace {
    pub fn new(size: usize, cache_duration: u64, name: String, number_of_dumps: u32) -> CacheSpace {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(RwLock::new(AHashMap::new()));
        }

        CacheSpace {
            data: data.into_boxed_slice(),
            size,
            cache_duration,
            number_of_dumps: AtomicU32::new(number_of_dumps),
            name
        }
    }
}

impl SpaceInterface for CacheSpace {
    #[inline(always)]
    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        match self.data[get_hash(key) % self.size].read().unwrap().get(key) {
            Some(value) => Some(value.1.clone()),
            None => None,
        }
    }

    #[inline(always)]
    fn get_and_reset_cache_time(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        match self.data[get_hash(key) % self.size].write().unwrap().get_mut(key) {
            Some(value) => {
                value.0 = NOW_MINUTES.load(SeqCst);
                Some(value.1.clone())
            },
            None => None,
        }
    }

    #[inline(always)]
    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        self.data[get_hash(&key) % self.size].write().unwrap().insert(key, (NOW_MINUTES.load(SeqCst), value));
    }

    #[inline(always)]
    fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        self.data[get_hash(&key) % self.size].write().unwrap().entry(key).or_insert((NOW_MINUTES.load(SeqCst), value));
    }

    #[inline(always)]
    fn delete(&self, key: &Vec<u8>) {
        self.data[get_hash(key) % self.size].write().unwrap().remove(key);
    }

    #[inline(always)]
    fn count(&self) -> u64 {
        let mut count = 0;
        for i in 0..self.size {
            let mut part_of_space = self.data[i].write().unwrap();
            count += part_of_space.len();
        }
        count as u64
    }

    fn dump(&self) {}
    fn rise(&self) {}

    #[inline(always)]
    fn invalid_cache(&self) {
        let now = NOW_MINUTES.load(SeqCst);
        let duration = self.cache_duration;
        for i in 0..self.size {
            let mut part_of_space = self.data[i].write().unwrap();
            part_of_space.retain(|_, value| {
                return value.0 + duration > now;
            });
        }
    }
}