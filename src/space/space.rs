pub trait SpaceInterface {
    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>>;
    fn get_and_reset_cache_time(&self, key: &Vec<u8>) -> Option<Vec<u8>>;
    fn set(&self, key: Vec<u8>, value: Vec<u8>);
    fn insert(&self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&self, key: &Vec<u8>);
    fn count(&self) -> u64;

    fn dump(&self);
    fn rise(&self);

    fn invalid_cache(&self);
}

pub type SpaceEngineType = u8;
pub const CACHE: SpaceEngineType = 0;
pub const IN_MEMORY: SpaceEngineType = 1;
pub const ON_DISK: SpaceEngineType = 2;