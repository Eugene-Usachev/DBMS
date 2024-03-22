use crate::index::{index::SIZE, Index};
use ahash::RandomState;
use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash, Hasher},
    sync::RwLock,
};

pub struct HashInMemoryIndex<K, V>
where
    K: Eq + Hash,
    V: Eq + Clone,
{
    pub data: Box<[RwLock<HashMap<K, V>>]>,
    pub lob: usize,
    pub state: RandomState,
}

impl<K, V> HashInMemoryIndex<K, V>
where
    K: Eq + Hash,
    V: Eq + Clone,
{
    pub fn new() -> Self {
        let state = RandomState::new();
        let mask = f64::log2(SIZE as f64) as u32;
        let lob = (1 << mask) - 1;
        let mut vec = Vec::with_capacity(SIZE);
        for _ in 0..SIZE {
            vec.push(RwLock::new(HashMap::new()));
        }
        Self {
            data: vec.into_boxed_slice(),
            state,
            lob,
        }
    }
    #[inline(always)]
    fn get_number(&self, key: &K) -> usize {
        let mut hasher = self.state.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize & self.lob
    }
}

impl<K, V> Index<K, V> for HashInMemoryIndex<K, V>
where
    K: Eq + Hash,
    V: Eq + Clone,
{
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        let mut shard = self.data[self.get_number(&key)].write().unwrap();
        if shard.contains_key(&key) {
            return false;
        }
        shard.insert(key, value);
        true
    }

    #[inline(always)]
    fn set(&self, key: K, value: V) -> Option<V> {
        self.data[self.get_number(&key)]
            .write()
            .unwrap()
            .insert(key, value)
    }
    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        self.data[self.get_number(key)]
            .read()
            .unwrap()
            .get(key)
            .cloned()
    }

    #[inline(always)]
    fn get_and_modify<F>(&self, key: &K, mut f: F) -> Option<V>
    where
        F: FnMut(&mut V),
    {
        let mut shard = self.data[self.get_number(key)].write().unwrap();
        let Some(res) = shard.get_mut(key) else {
            return None;
        };
        f(res);
        Some(res.clone())
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        self.data[self.get_number(key)].write().unwrap().remove(key)
    }
    #[inline(always)]
    fn contains(&self, key: &K) -> bool {
        self.data[self.get_number(key)]
            .read()
            .unwrap()
            .contains_key(key)
    }

    #[inline(always)]
    fn resize(&self, new_size: usize) {
        let size_for_shard = new_size / SIZE;
        for i in 0..self.data.len() {
            self.data[i].write().unwrap().reserve(size_for_shard);
        }
    }

    #[inline(always)]
    fn clear(&self) {
        for i in 0..self.data.len() {
            self.data[i].write().unwrap().clear();
        }
    }

    fn count(&self) -> usize {
        let mut l = 0;
        for i in 0..self.data.len() {
            l += self.data[i].read().unwrap().len();
        }
        return l;
    }

    #[inline(always)]
    fn for_each<F>(&self, f: F)
    where
        F: Fn(&K, &V),
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].read().unwrap().iter() {
                f(k, v);
            }
        }
    }

    #[inline(always)]
    fn for_each_mut<F>(&self, mut f: F)
    where
        F: FnMut(&K, &mut V),
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].write().unwrap().iter_mut() {
                f(k, v);
            }
        }
    }

    #[inline(always)]
    fn retain<F>(&self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool + Clone,
    {
        for shard in self.data.iter() {
            let mut shard = shard.write().unwrap();
            shard.retain(f.clone());
        }
    }
}

unsafe impl<K, V> Send for HashInMemoryIndex<K, V>
where
    K: Eq + Hash,
    V: Eq + Clone,
{
}

unsafe impl<K, V> Sync for HashInMemoryIndex<K, V>
where
    K: Eq + Hash,
    V: Eq + Clone,
{
}
