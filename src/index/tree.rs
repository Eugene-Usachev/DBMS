use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::intrinsics::unlikely;
use std::sync::RwLock;
use ahash::RandomState;

use crate::index::Index;
use crate::index::index::SIZE;

pub struct TreeInMemoryIndex<K, V>
    where K: Eq + Ord + Hash, V: Eq + Clone
{
    data: Box<[RwLock<BTreeMap<K, V>>]>,
    lob: usize,
    state: RandomState
}

impl<K, V> TreeInMemoryIndex<K, V>
    where K: Eq + Ord + Hash, V: Eq + Clone
{
    pub(crate) fn new() -> Self {
        let state = RandomState::new();
        let mask = f64::log2(SIZE as f64) as u32;
        let lob = (1 << mask) - 1;
        let mut vec = Vec::with_capacity(SIZE);
        for _ in 0..SIZE {
            vec.push(RwLock::new(BTreeMap::new()));
        }
        Self {
            data: vec.into_boxed_slice(),
            state,
            lob
        }
    }
    #[inline(always)]
    fn get_number(&self, key: &K) -> usize {
        let mut hasher = self.state.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize & self.lob
    }
}

impl<K, V> Index<K, V> for TreeInMemoryIndex<K, V>
    where K: Eq + Ord + Hash, V: Eq + Clone
{
    #[inline(always)]
    fn insert(&self, key: K, value: V) -> bool {
        let mut shard = self.data[self.get_number(&key)].write().unwrap();
        if unlikely(shard.contains_key(&key)) {
            return false;
        }
        shard.insert(key, value);
        true
    }

    #[inline(always)]
    fn set(&self, key: K, value: V) -> Option<V> {
        self.data[self.get_number(&key)].write().unwrap().insert(key, value)
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        self.data[self.get_number(key)].read().unwrap().get(key).cloned()
    }

    #[inline(always)]
    fn get_and_modify<F>(&self, key: &K, mut f: F) -> Option<V> where F: FnMut(&mut V) {
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
        self.data[self.get_number(key)].read().unwrap().contains_key(key)
    }

    #[inline(always)]
    fn resize(&self, new_size: usize) {
        // All is ok. Nothing to do
    }

    #[inline(always)]
    fn clear(&self) {
        for i in 0..self.data.len() {
            self.data[i].write().unwrap().clear();
        }
    }

    #[inline(always)]
    fn count(&self) -> usize {
        let mut l = 0;
        for i in 0..self.data.len() {
            l += self.data[i].read().unwrap().len();
        }
        return l;
    }

    #[inline(always)]
    fn for_each<F>(&self, f: F)
        where F: Fn(&K, &V)
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].read().unwrap().iter() {
                f(k, v);
            }
        }
    }

    #[inline(always)]
    fn for_each_mut<F>(&self, mut f: F)
        where F: FnMut(&K, &mut V)
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].write().unwrap().iter_mut() {
                f(k, v);
            }
        }
    }

    #[inline(always)]
    fn retain<F>(&self, f: F)
        where F: FnMut(&K, &mut V) -> bool + Clone
    {
        for i in 0..self.data.len() {
            self.data[i].write().unwrap().retain(f.clone());
        }
    }
}

unsafe impl<K, V> Send for TreeInMemoryIndex<K, V>
    where K: Eq + Ord + Hash, V: Eq + Clone
{}

unsafe impl<K, V> Sync for TreeInMemoryIndex<K, V>
    where K: Eq + Ord + Hash, V: Eq + Clone
{}