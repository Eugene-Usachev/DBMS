use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex, RwLock};
use std::{env, thread};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::space::space::SpaceInterface;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use crate::constants;

pub static NOW_MINUTES: AtomicU64 = AtomicU64::new(0);

pub struct Storage {
    pub(crate) spaces: RwLock<Vec<Box<dyn SpaceInterface + Send + Sync + 'static>>>,
    pub(crate) spaces_names: RwLock<Vec<String>>,

    pub log_file: Mutex<File>,
    pub log_file_number: AtomicU32,

    pub main_file: Mutex<File>,

    pub cache_spaces_indexes: RwLock<Vec<usize>>,
    dump_interval: u32,
}

impl Storage {
    pub(crate) fn new() -> Self {
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR].iter().collect();
        std::fs::create_dir_all(path).expect("[Error] Failed to create the persistence directory");
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
        let file_name = "storage";
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, file_name].iter().collect();
        let main_file = Mutex::new(File::create(path).expect("[Error] Failed to create the main storage file"));

        let file_name = format!("log{}.log", 0);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();

        Self {
            spaces: RwLock::new(Vec::with_capacity(1)),
            spaces_names: RwLock::new(Vec::with_capacity(1)),
            cache_spaces_indexes: RwLock::new(Vec::with_capacity(1)),
            main_file,
            log_file_number: AtomicU32::new(0),
            log_file: Mutex::new(File::create(path).unwrap()),
            dump_interval
        }
    }

    fn dump(storage: Arc<Self>) {
        let log_number = storage.log_file_number.fetch_add(1, SeqCst);
        let file_name = format!("log_number.txt");
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.write_all(&[log_number as u8, (log_number >> 8) as u8, (log_number >> 16) as u8, (log_number >> 24) as u8]).unwrap();

        {
            let file_name = format!("log{}.log", log_number + 1);
            let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
            let file = File::create(path).unwrap();
            *storage.log_file.lock().unwrap() = file;
        }
        let join = thread::spawn(move || {
            let spaces = storage.spaces.read().unwrap();
            for space in spaces.iter() {
                space.dump();
            }
        });
        join.join().unwrap();

        let file_name = format!("log{}.log", log_number);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        std::fs::remove_file(path).expect("[Error] Failed to remove log file");
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
                    println!("Starting dump");
                    let start = Instant::now();
                    Self::dump(storage.clone());
                    let elapsed = start.elapsed();
                    println!("Dump took {:?} seconds", elapsed);
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