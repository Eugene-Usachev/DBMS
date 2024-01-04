use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, RwLock};
use std::{env, thread};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::table::table::Table;
use std::fs::{File, OpenOptions};
use std::intrinsics::unlikely;
use std::io::{Read, Write};
use std::path::PathBuf;
use crate::bin_types::{BinKey, BinValue};
use crate::constants;
use crate::constants::actions::*;
use crate::index::HashInMemoryIndex;
use crate::table::cache::CacheTable;
use crate::table::in_memory::InMemoryTable;
use crate::table::on_disk::OnDiskTable;
use crate::utils::fastbytes::uint;
use crate::writers::{LogFile};

pub static NOW_MINUTES: AtomicU64 = AtomicU64::new(0);

pub struct Storage {
    pub tables: RwLock<Vec<Box<dyn Table + 'static>>>,
    pub tables_names: RwLock<Vec<String>>,

    //pub log_writer: Arc<PipeWriter>,
    pub log_file: LogFile,
    pub log_file_number: AtomicU32,

    pub cache_tables_indexes: RwLock<Vec<usize>>,
    pub dump_interval: u32,
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
        let log_number = Self::get_log_file_number();
        let file_name = format!("log{log_number}.log", );
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        let log_file = LogFile::new(log_file);

        Self {
            tables: RwLock::new(Vec::with_capacity(1)),
            tables_names: RwLock::new(Vec::with_capacity(1)),
            cache_tables_indexes: RwLock::new(Vec::with_capacity(1)),
            log_file_number: AtomicU32::new(log_number as u32),
            //log_writer: Arc::new(PipeWriter::new(path.to_str().unwrap().to_string())),
            log_file,
            dump_interval
        }
    }

    fn dump(storage: Arc<Self>) {
        let old_log_number = storage.log_file_number.fetch_add(1, SeqCst);
        let log_number = old_log_number + 1;
        let file_name = "persistence.txt".to_string();
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        file.write_all(&[log_number as u8, (log_number >> 8) as u8, (log_number >> 16) as u8, (log_number >> 24) as u8]).unwrap();

        {
            let file_name = format!("log{}.log", log_number);
            let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
            let file = File::create(path).unwrap();
            *storage.log_file.file.lock().unwrap() = file;
        }
        let join = thread::spawn(move || {
            let spaces = storage.tables.read().unwrap();
            for (number, space) in spaces.iter().enumerate() {
                space.dump();
                file.write_all(&[number as u8, (number >> 8) as u8, (number >> 16) as u8, (number >> 24) as u8]).unwrap();
            }
        });
        join.join().unwrap();

        let file_name = format!("log{}.log", old_log_number);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        std::fs::remove_file(path).expect("[Error] Failed to remove log file");
    }

    pub fn init(storage: Arc<Self>) {
        Self::read_log(storage.clone());

        let since_the_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards").as_secs() / 60;

        NOW_MINUTES.store(since_the_epoch, SeqCst);

        let dump_interval = storage.dump_interval;
        tokio::spawn(async move {
            let mut dump_after = dump_interval;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;

                dump_after -= 1;
                if unlikely(dump_after == 0) {
                    let storage = storage.clone();
                    tokio::spawn(async move {
                        println!("Starting dump");
                        let start = Instant::now();
                        Self::dump(storage.clone());
                        let elapsed = start.elapsed();
                        println!("Dump took {:?} seconds", elapsed);
                    });
                    dump_after = dump_interval;
                }

                let since_the_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards").as_secs() / 60;
                NOW_MINUTES.store(since_the_epoch, SeqCst);

                let storage = storage.clone();
                tokio::spawn(async move {
                    let cache_spaces_indexes = storage.cache_tables_indexes.read().unwrap();
                    let tables = storage.tables.read().unwrap();
                    for index in cache_spaces_indexes.iter() {
                        let table = tables.get(*index).unwrap();
                        table.invalid_cache();
                    }
                });
            }
        });
    }

    fn get_log_file_number() -> usize {
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, "persistence.txt"].iter().collect();
        let mut file_ = match File::open(path) {
            Ok(file) => file,
            Err(_) => {
                return 0;
            }
        };

        let mut buf = [0u8; 4];
        file_.read_exact(&mut buf).unwrap();
        let log_number = uint::u32(&buf);
        return log_number as usize;
    }

    pub fn read_log(storage: Arc<Self>) {
        #[inline(always)]
        fn read_more(chunk: &mut [u8], start_offset: usize, bytes_read: usize, offset_last_record: &mut usize) {
            let slice_to_copy = &mut Vec::with_capacity(0);
            chunk[start_offset..bytes_read].clone_into(slice_to_copy);
            *offset_last_record = bytes_read - start_offset;
            chunk[0..*offset_last_record].copy_from_slice(slice_to_copy);
        }

        let log_number = Self::get_log_file_number();

        let file_name = format!("log{}.log", log_number);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        let mut input = match File::open(path) {
            Ok(file) => file,
            Err(_) => {
                return;
            }
        };

        let file_len = input.metadata().unwrap().len();
        let mut chunk = [0u8; 64 * 1024];
        let mut total_read = 0;
        let mut offset_last_record = 0;
        let mut offset;
        let mut start_offset = 0;
        let mut action;
        let mut kl;
        let mut key_offset;
        let mut name_len;
        let mut name_offset;
        let mut name;
        let mut is_it_logging;
        let mut vl;
        let mut value_offset;
        let mut number;
        let mut cache_duration;
        let mut tables = storage.tables.write().unwrap();

        'read: loop {
            if unlikely(total_read == file_len) {
                break;
            }
            let mut bytes_read = input.read(&mut chunk[offset_last_record..]).expect("Failed to read");
            if unlikely(bytes_read == 0) {
                break;
            }


            bytes_read += offset_last_record;
            offset = 0;
            total_read += bytes_read as u64;

            loop {
                if unlikely(offset + 1 > bytes_read) {
                    read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                    continue 'read;
                }
                start_offset = offset;
                action = chunk[offset];
                offset += 1;
                match action {
                    INSERT => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        number = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;

                        if unlikely(offset + number as usize + 1 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        kl = chunk[offset] as u32;
                        offset += 1;
                        if unlikely(kl == 255) {
                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            kl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                            offset += 2;
                        }

                        if unlikely(offset + kl as usize + 2 /*for vl*/ > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        key_offset = offset;
                        offset += kl as usize;

                        vl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        offset += 2;
                        if unlikely(vl == 65535) {
                            if unlikely(offset + 4 > bytes_read) {
                                read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            vl = (chunk[offset + 3] as u32) << 24 | (chunk[offset + 2] as u32) << 16 | (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                            offset += 4;
                        }

                        if unlikely(offset + vl as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        value_offset = offset;
                        offset += vl as usize;

                        tables[number as usize].insert_without_log(BinKey::new(&chunk[key_offset..key_offset+kl as usize]), BinValue::new(&chunk[value_offset..offset]));
                    }
                    SET => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        number = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;

                        if unlikely(offset + number as usize + 1 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        kl = chunk[offset] as u32;
                        offset += 1;
                        if unlikely(kl == 255) {
                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            kl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                            offset += 2;
                        }

                        if unlikely(offset + kl as usize + 2 /*for vl*/ > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        key_offset = offset;
                        offset += kl as usize;

                        vl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        offset += 2;
                        if unlikely(vl == 65535) {
                            if unlikely(offset + 4 > bytes_read) {
                                read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            vl = (chunk[offset + 3] as u32) << 24 | (chunk[offset + 2] as u32) << 16 | (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                            offset += 4;
                        }

                        if unlikely(offset + vl as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        value_offset = offset;
                        offset += vl as usize;

                        tables[number as usize].set_without_log(BinKey::new(&chunk[key_offset..key_offset+kl as usize]), BinValue::new(&chunk[value_offset..offset]));
                    }
                    DELETE => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        number = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;

                        if unlikely(offset + number as usize + 1 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        kl = chunk[offset] as u32;
                        offset += 1;
                        if unlikely(kl == 255) {
                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            kl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                            offset += 2;
                        }

                        if unlikely(offset + kl as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        key_offset = offset;
                        offset += kl as usize;

                        tables[number as usize].delete_without_log(&BinKey::new(&chunk[key_offset..key_offset+kl as usize]));
                    }
                    CREATE_SPACE_IN_MEMORY => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        number = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;

                        if unlikely(offset + 1 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        
                        is_it_logging = chunk[offset] != 0;
                        offset += 1;
                        
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        } 
                        
                        name_len = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        offset += 2;

                        if unlikely(offset + name_len as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        name_offset = offset;
                        offset += name_len as usize;
                        name = vec![0; name_len as usize];
                        name.copy_from_slice(&chunk[name_offset..offset]);

                        let number = tables.len();

                        tables.push(
                            Box::new(InMemoryTable::new(number as u16, HashInMemoryIndex::new(),
                                                        String::from_utf8(name).unwrap(), is_it_logging ,0)));
                    }
                    CREATE_SPACE_ON_DISK => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        number = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;

                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        name_len = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        offset += 2;

                        if unlikely(offset + name_len as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        name_offset = offset;
                        offset += name_len as usize;
                        name = vec![0; name_len as usize];
                        name.copy_from_slice(&chunk[name_offset..offset]);

                        tables.push(
                            Box::new(OnDiskTable::new(String::from_utf8(name).unwrap(), 512, HashInMemoryIndex::new())));
                    }
                    CREATE_SPACE_CACHE => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        number = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;

                        if unlikely(offset + 1 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        is_it_logging = chunk[offset] != 0;
                        offset += 1;

                        if unlikely(offset + 8 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        cache_duration = (chunk[offset + 7] as u64) << 56 | (chunk[offset + 6] as u64) << 48 | (chunk[offset + 5] as u64) << 40 | (chunk[offset + 4] as u64) << 32 | (chunk[offset + 3] as u64) << 24 | (chunk[offset + 2] as u64) << 16 | (chunk[offset + 1] as u64) << 8 | (chunk[offset] as u64);
                        offset += 8;

                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        name_len = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        offset += 2;

                        if unlikely(offset + name_len as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        name_offset = offset;
                        offset += name_len as usize;
                        name = vec![0; name_len as usize];
                        name.copy_from_slice(&chunk[name_offset..offset]);

                        let number = tables.len();

                        tables.push(
                            Box::new(CacheTable::new(number as u16, HashInMemoryIndex::new(), cache_duration,
                                                        String::from_utf8(name).unwrap(), is_it_logging ,0)));
                    }
                    _ => {
                        panic!("Unknown action was detected while reading the log: {}", action);
                    }
                }
            }
        }
    }
}