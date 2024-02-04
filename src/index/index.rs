pub trait Index<K, V> {
    /// Inserts a key-value pair into the index. Do nothing if the key already exists.
    ///
    /// Returns `true` if inserted, `false` otherwise.
    fn insert(&mut self, key: K, value: V) -> bool;
    fn set(&mut self, key: K, value: V) -> Option<V>;
    fn get(&self, key: &K) -> Option<V>;
    fn get_and_modify<F>(&mut self, key: &K, f: F) -> Option<V> where F: FnMut(&mut V);
    fn remove(&mut self, key: &K) -> Option<V>;
    fn contains(&self, key: &K) -> bool;
    fn clear(&mut self);
    fn resize(&mut self, new_size: usize);
    fn count(&self) -> usize;
    fn for_each<F>(&self, f: F) where F: Fn(&K, &V);
    fn for_each_mut<F>(&mut self, f: F) where F: FnMut(&K, &mut V);
    fn retain<F>(&mut self, f: F) where F: FnMut(&K, &mut V) -> bool + Clone;
}

pub const SIZE: usize = 512;
pub const SIZE_U64: u64 = SIZE as u64;

#[repr(u8)]
pub enum IndexType {
    Hash = 0u8,
    BTree = 1u8,
    Serial = 2u8,
}