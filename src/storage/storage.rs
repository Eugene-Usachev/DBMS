use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, RwLock};
use std::{env, thread};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::space::space::SpaceInterface;
use std::borrow::Borrow;

pub static NOW_MINUTES: AtomicU64 = AtomicU64::new(0);

pub struct Storage {
    pub(crate) spaces: RwLock<Vec<Box<dyn SpaceInterface + Send + Sync + 'static>>>,
    pub(crate) spaces_names: RwLock<Vec<String>>,
    pub cache_spaces_indexes: RwLock<Vec<usize>>,
    dump_interval: u32,
}

impl Storage {
    pub(crate) fn new() -> Self {
        let dump_interval = match env::var("DUMP_INTERVAL") {
            Ok(value) => {
                println!("The dump interval was set to: {} minutes using the environment variable \"DUMP_INTERVAL\"", value);
                value.parse().expect("[Panic] The dump interval must be a number!")
            },
            Err(_) => {
                println!("The dump interval was not set using the environment variable \"DUMP_INTERVAL\", setting it to 60 minutes");
                60
            }
        };

        Self {
            spaces: RwLock::new(Vec::with_capacity(1)),
            spaces_names: RwLock::new(Vec::with_capacity(1)),
            cache_spaces_indexes: RwLock::new(Vec::with_capacity(1)),
            dump_interval
        }
    }

    fn dump(storage: Arc<Self>) {
        let join = thread::spawn(move || {
            let spaces = storage.spaces.read().unwrap();
            for space in spaces.iter() {
                space.dump();
            }
        });
        join.join().unwrap();
    }

    #[inline(always)]
    pub fn init(storage: Arc<Self>) {
        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards").as_secs() / 60;

        NOW_MINUTES.store(since_the_epoch, SeqCst);

        let dump_interval = storage.dump_interval;

        thread::spawn(move || {
            let mut dump_after = dump_interval;
            loop {
                thread::sleep(Duration::from_secs(60));

                dump_after -= 1;
                if dump_after == 0 {
                    Self::dump(storage.clone());
                    dump_after = dump_interval;
                }

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