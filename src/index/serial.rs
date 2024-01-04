use std::intrinsics::{likely, unlikely};
use std::mem;
use std::sync::RwLock;
use ahash::RandomState;
use crate::index::{Index};
use crate::index::index::{SIZE, SIZE_U64};


/// TODO DOESN't WORK!
pub struct SerialInMemoryIndex<V>
    where V: Eq + Clone
{
    pub data: Box<[RwLock<Vec<V>>]>,
    pub mask: u64,
    pub state: RandomState
}

impl<V> SerialInMemoryIndex<V>
    where V: Eq + Clone
{
    pub(crate) fn new() -> Self {
        let state = RandomState::new();
        let lob = f64::log2(SIZE as f64) as u32;
        let mask = (1 << lob) - 1;
        let mut vec = Vec::with_capacity(SIZE);
        for _ in 0..SIZE {
            vec.push(RwLock::new(Vec::with_capacity(4096)));
        }
        Self {
            data: vec.into_boxed_slice(),
            state,
            mask
        }
    }
    #[inline(always)]
    pub fn get_number(&self, key: u64) -> usize {
        (key & self.mask) as usize
    }
}

#[inline(always)]
pub fn set_last_two_bytes_to_zero(num: u64) -> u64 {
    //num &= !(0xFFFF << ((8 - 2) * 8));
    num << 16
}

impl<V> Index<u64, V, > for SerialInMemoryIndex<V>
    where V: Eq + Clone + std::fmt::Debug
{
    #[inline(always)]
    fn insert(&self, key: u64, value: V) -> bool {
        let key = set_last_two_bytes_to_zero(key);
        let mut shard = self.data[self.get_number(key)].write().unwrap();
        if unlikely(shard.contains(&value)) {
            return false;
        }
        shard.push(value);
        true
    }

    #[inline(always)]
    fn set(&self, key: u64, mut value: V) -> Option<V> {
        let key = set_last_two_bytes_to_zero(key);
        let key_usize = (key / SIZE_U64) as usize;
        let mut shard = self.data[self.get_number(key)].write().unwrap();
        let res = shard.get_mut(key_usize);
        if unlikely(res.is_some()) {
            mem::swap(res.unwrap(), &mut value);
            return Some(value);
        }
        None
    }
    #[inline(always)]
    fn get(&self, key: &u64) -> Option<V> {
        let key = set_last_two_bytes_to_zero(*key);
        let key_usize = (key / SIZE_U64) as usize;
        self.data[self.get_number(key)].read().unwrap().get(key_usize).cloned()
    }

    #[inline(always)]
    fn get_and_modify<F>(&self, key: &u64, mut f: F) -> Option<V> where F: FnMut(&mut V) {
        let key = set_last_two_bytes_to_zero(*key);
        let key_usize = (key / SIZE_U64) as usize;
        let mut shard = self.data[self.get_number(key)].write().unwrap();
        let Some(res) = shard.get_mut(key_usize) else {
            return None;
        };
        f(res);
        Some(res.clone())
    }

    #[inline(always)]
    fn remove(&self, key: &u64) -> Option<V> {
        let key = set_last_two_bytes_to_zero(*key);
        let key_usize = (key / SIZE_U64) as usize;
        let mut v = self.data[self.get_number(key)].write().unwrap();
        if likely(v.len() > key_usize) {
            return Some(v.remove(key_usize));
        }
        None
    }
    #[inline(always)]
    fn contains(&self, key: &u64) -> bool {
        let key = set_last_two_bytes_to_zero(*key);
        let key_usize = (key / SIZE_U64) as usize;
        self.data[self.get_number(key)].read().unwrap().get(key_usize).is_some()
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
        where F: Fn(&u64, &V)
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].read().unwrap().iter().enumerate() {
                f(&(k as u64), v);
            }
        }
    }

    #[inline(always)]
    fn for_each_mut<F>(&self, mut f: F)
        where F: FnMut(&u64, &mut V)
    {
        for i in 0..self.data.len() {
            for (k, v) in self.data[i].write().unwrap().iter_mut().enumerate() {
                // TODO key
                f(&(k as u64), v);
            }
        }
    }

    #[inline(always)]
    fn retain<F>(&self, mut f: F)
        where F: FnMut(&u64, &mut V) -> bool + Clone
    {
        for i in 0..self.data.len() {
            let c = &mut 0u64;
            self.data[i].write().unwrap().retain_mut(|value| {
                let count = *c;
                let is_keep = f(&count, value);
                *c = count + 1;
                return is_keep;
            });
        }
    }
}

unsafe impl<V> Send for SerialInMemoryIndex<V>
    where V: Eq + Clone
{}

unsafe impl<V> Sync for SerialInMemoryIndex<V>
    where V: Eq + Clone
{}