pub trait Space {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn get_and_reset_cache_time(&self, key:  &[u8]) -> Option<Vec<u8>>;
    fn set(&self, key: Vec<u8>, value: Vec<u8>, log_buffer: &mut [u8], log_buffer_offset: &mut usize);
    fn insert(&self, key: Vec<u8>, value: Vec<u8>, log_buffer: &mut [u8], log_buffer_offset: &mut usize);
    fn delete(&self, key: Vec<u8>, log_buffer: &mut [u8], log_buffer_offset: &mut usize);
    fn count(&self) -> u64;

    fn dump(&self);
    fn rise(&mut self);
    fn invalid_cache(&self);
}

pub type SpaceEngineType = u8;
pub const CACHE: SpaceEngineType = 0;
pub const IN_MEMORY: SpaceEngineType = 1;
pub const ON_DISK: SpaceEngineType = 2;