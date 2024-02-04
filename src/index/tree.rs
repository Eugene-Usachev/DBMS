use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash, Hasher};
use ahash::RandomState;

use crate::index::Index;
use crate::index::index::SIZE;

pub struct TreeInMemoryIndex<K, V>
    where K: Eq + Ord + Hash, V: Eq + Clone
{
    data: Box<[BTreeMap<K, V>]>,
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
            vec.push(BTreeMap::new());
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
    fn insert(&mut self, key: K, value: V) -> bool {
        let shard = &mut self.data[self.get_number(&key)];
        if shard.contains_key(&key) {
            return false;
        }
        shard.insert(key, value);
        true
    }

    #[inline(always)]
    fn set(&mut self, key: K, value: V) -> Option<V> {
        self.data[self.get_number(&key)].insert(key, value)
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        self.data[self.get_number(key)].get(key).cloned()
    }

    #[inline(always)]
    fn get_and_modify<F>(&mut self, key: &K, mut f: F) -> Option<V> where F: FnMut(&mut V) {
        let shard = &mut self.data[self.get_number(key)];
        let Some(res) = shard.get_mut(key) else {
            return None;
        };
        f(res);
        Some(res.clone())
    }

    #[inline(always)]
    fn remove(&mut self, key: &K) -> Option<V> {
        self.data[self.get_number(key)].remove(key)
    }

    #[inline(always)]
    fn contains(&self, key: &K) -> bool {
        self.data[self.get_number(key)].contains_key(key)
    }

    #[inline(always)]
    fn resize(&mut self, _new_size: usize) {
        // All is ok. Nothing to do
    }

    #[inline(always)]
    fn clear(&mut self) {
        for i in 0..self.data.len() {
            self.data[i].clear();
        }
    }

    #[inline(always)]
    fn count(&self) -> usize {
        let mut l = 0;
        for i in 0..self.data.len() {
            l += self.data[i].len();
        }
        return l;
    }

    #[inline(always)]
    fn for_each<F>(&self, f: F)
        where F: Fn(&K, &V)
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].iter() {
                f(k, v);
            }
        }
    }

    #[inline(always)]
    fn for_each_mut<F>(&mut self, mut f: F)
        where F: FnMut(&K, &mut V)
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].iter_mut() {
                f(k, v);
            }
        }
    }

    #[inline(always)]
    fn retain<F>(&mut self, f: F)
        where F: FnMut(&K, &mut V) -> bool + Clone
    {
        for i in 0..self.data.len() {
            self.data[i].retain(f.clone());
        }
    }
}