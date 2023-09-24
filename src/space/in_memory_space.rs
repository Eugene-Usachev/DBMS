use std::sync::RwLock;
use ahash::AHashMap;
use crate::space::space::SpaceInterface;
use crate::utils::hash::get_hash::get_hash;

pub struct InMemorySpace {
    pub data: Box<[RwLock<AHashMap<Vec<u8>, Vec<u8>>>]>,
    pub size: usize,
}

impl InMemorySpace {
    pub fn new(size: usize) -> InMemorySpace {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(RwLock::new(AHashMap::new()));
        }

        InMemorySpace {
            data: data.into_boxed_slice(),
            size
        }
    }
}

impl SpaceInterface for InMemorySpace {
    #[inline(always)]
    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        match self.data[get_hash(key) % self.size].read().unwrap().get(key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    #[inline(always)]
    fn set(&self, key: Vec<u8>,value: Vec<u8>) {
        self.data[get_hash(&key) % self.size].write().unwrap().insert(key, value);
    }

    #[inline(always)]
    fn insert(&self,  key: Vec<u8>, value: Vec<u8>) {
        self.data[get_hash(&key) % self.size].write().unwrap().entry(key).or_insert(value);
    }

    #[inline(always)]
    fn delete(&self,  key: &Vec<u8>) {
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

    // NOT EXISTS!

    fn get_and_reset_cache_time(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        unreachable!()
    }

    fn invalid_cache(&self) {
        unreachable!()
    }
}