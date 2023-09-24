use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::space::space::SpaceInterface;

pub static NOW_MINUTES: AtomicU64 = AtomicU64::new(0);

pub struct Storage {
    pub(crate) spaces: RwLock<Vec<Box<dyn SpaceInterface + Send + Sync + 'static>>>,
    pub(crate) spaces_names: RwLock<Vec<String>>,
    pub cache_spaces_indexes: RwLock<Vec<usize>>
}

impl Storage {
    pub(crate) fn new() -> Self {
        Self {
            spaces: RwLock::new(Vec::with_capacity(1)),
            spaces_names: RwLock::new(Vec::with_capacity(1)),
            cache_spaces_indexes: RwLock::new(Vec::with_capacity(1))
        }
    }

    #[inline(always)]
    pub fn start_cache_clearer(storage: Arc<Self>) {
        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards").as_secs() / 60;

        NOW_MINUTES.store(since_the_epoch, SeqCst);

        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(60));
                let since_the_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards").as_secs() / 60;
                NOW_MINUTES.store(since_the_epoch, SeqCst);
                let cache_spaces_indexes = storage.cache_spaces_indexes.read().unwrap();
                let spaces = storage.spaces.read().unwrap();
                for index in cache_spaces_indexes.iter() {
                    let space =spaces.get(*index).unwrap();
                    space.invalid_cache();
                }
            }
        });
    }
}