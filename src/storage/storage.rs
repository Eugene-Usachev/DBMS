use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::{env, thread};
use std::cell::UnsafeCell;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::table::table::{Table, TableEngine};
use std::fs::{File, OpenOptions};
use std::intrinsics::{unlikely};
use std::io::{Read, Write};
use std::path::PathBuf;
use crate::bin_types::{BinKey, BinValue};
use crate::constants;
use crate::constants::actions::*;
use crate::index::{HashInMemoryIndex, Index};
use crate::scheme::scheme::{empty_scheme, Scheme, scheme_from_bytes, SchemeJSON};
use crate::table::cache::CacheTable;
use crate::table::in_memory::InMemoryTable;
use crate::table::on_disk::OnDiskTable;
use crate::utils::fastbytes::uint;
use crate::writers::{LogFile};

pub static NOW_MINUTES: AtomicU64 = AtomicU64::new(0);

pub struct Storage {
    /// SAFETY:
    ///
    /// 1 - The tables vector has the same lifetime as the storage and keep the capacity all lifetime of the storage;
    ///
    /// 2 - We never delete the tables from the tables vector, only mark them as deleted;
    ///
    /// 3 - We never change table numbers;
    ///
    /// 4 - We push new tables only when tables_names is locked.
    pub tables: UnsafeCell<Vec<Box<dyn Table + 'static>>>,
    pub tables_names: RwLock<Vec<String>>,
    pub number_of_dumps: Arc<AtomicU32>,
    pub last_tables_count: AtomicU32,

    //pub log_writer: Arc<PipeWriter>,
    pub log_file: LogFile,
    pub table_configs_file_path: PathBuf,
    pub number_of_dumps_file_path: PathBuf,

    pub cache_tables_indexes: RwLock<Vec<usize>>,
    pub dump_interval: u32,
}

unsafe impl Send for Storage {}
unsafe impl Sync for Storage {}

impl Storage {
    pub fn new() -> Self {
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

        let number_of_dumps_file_path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, "number_of_dumps.bin"].iter().collect();
        let file = OpenOptions::new().read(true).open(number_of_dumps_file_path.clone());
        let number_of_dumps;
        if file.is_err() {
            number_of_dumps = 0;
            let file = File::create(number_of_dumps_file_path.clone());
            if file.is_err() {
                panic!("[Panic] Failed to create number of dumps file");
            }
            file.unwrap().write_all(&[0, 0, 0, 0]).unwrap();
        } else {
            let mut file = file.unwrap();
            let mut buf = [0u8; 4];
            file.read_exact(&mut buf).unwrap();
            number_of_dumps = uint::u32(&buf);
        }

        let file_name = "tables.bin";
        let table_configs_file_path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, file_name].iter().collect();
        OpenOptions::new().append(true).create(true).open(table_configs_file_path.clone()).expect("[Error] Failed to open table configs file");
        let log_number = Self::get_log_file_number(number_of_dumps_file_path.clone());
        let file_name = format!("log{log_number}.bin", );
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        let log_file = LogFile::new(log_file);

        Self {
            tables: UnsafeCell::new(Vec::with_capacity(4096)),
            tables_names: RwLock::new(Vec::with_capacity(1)),
            cache_tables_indexes: RwLock::new(Vec::with_capacity(1)),
            number_of_dumps: Arc::new(AtomicU32::new(number_of_dumps)),
            last_tables_count: AtomicU32::new(0),
            //log_writer: Arc::new(PipeWriter::new(path.to_str().unwrap().to_string())),
            log_file,
            table_configs_file_path,
            number_of_dumps_file_path,
            dump_interval
        }
    }

    pub fn dump(storage: Arc<Self>) {
        let old_number_of_dumps = storage.number_of_dumps.fetch_add(1, SeqCst);
        let number_of_dumps = old_number_of_dumps + 1;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(storage.number_of_dumps_file_path.clone())
            .unwrap();
        file.write_all(&[number_of_dumps as u8, (number_of_dumps >> 8) as u8, (number_of_dumps >> 16) as u8, (number_of_dumps >> 24) as u8]).unwrap();

        {
            let file_name = format!("log{}.bin", number_of_dumps);
            let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
            let file = File::create(path).unwrap();
            *storage.log_file.file.lock().unwrap() = file;
        }

        let storage_for_dump = storage.clone();
        let last_tables_count = storage_for_dump.last_tables_count.load(SeqCst);
        let join = thread::spawn(move || {
            let tables;
            unsafe {
                tables = &*storage_for_dump.tables.get();
            }
            for (number, table) in tables.iter().enumerate() {
                if number as u32 >= last_tables_count {
                    let engine = table.engine();
                    match engine {
                        TableEngine::InMemory => {
                            Self::write_in_memory_table_on_disk(storage_for_dump.clone(), &table.name(), number, table.is_it_logging(), &table.user_scheme());
                        }
                        TableEngine::OnDisk => {
                            Self::write_on_disk_table_on_disk(storage_for_dump.clone(), &table.name(), number, &table.user_scheme());
                        }
                        TableEngine::CACHE => {
                            Self::write_cache_table_on_disk(storage_for_dump.clone(), &table.name(), number, table.is_it_logging(), table.cache_duration(), &table.user_scheme());
                        }
                    }
                }
                table.dump();
                file.write_all(&[number as u8, (number >> 8) as u8, (number >> 16) as u8, (number >> 24) as u8]).unwrap();
            }
        });
        join.join().unwrap();

        unsafe {
            storage.last_tables_count.store((*storage.tables.get()).len() as u32, SeqCst);
        }

        let file_name = format!("log{}.bin", old_number_of_dumps);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &file_name].iter().collect();
        let _ = std::fs::remove_file(path);
    }

    pub fn init(storage: Arc<Self>) {
        Self::rise(storage.clone());

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
                    let cache_tables_indexes = storage.cache_tables_indexes.read().unwrap();
                    let tables;
                    unsafe {
                        tables = (&*storage.tables.get()) as &Vec<Box<dyn Table>>;
                    }
                    for index in cache_tables_indexes.iter() {
                        let table = tables.get(*index).unwrap();
                        table.invalid_cache();
                    }
                });
            }
        });
    }

    fn write_table_config_on_disk(storage: Arc<Self>, bin_config: &[u8]) {
        let mut file_ = OpenOptions::new()
            .append(true)
            .create(true)
            .open(storage.table_configs_file_path.clone());
        if file_.is_err() {
            println!("Can't open table config file. Creating new one.");
            file_ = File::create(storage.table_configs_file_path.clone());
            if file_.is_err() {
                println!("Can't create table config file.");
                return;
            }
        }

        let mut file = file_.unwrap();
        file.write_all(bin_config).unwrap();
    }

    fn insert_table_name_and_get_number(tables_names: &mut RwLockWriteGuard<Vec<String>>, name: &str) -> (usize, bool) {
        let len = tables_names.len();
        for i in 0..len {
            if tables_names[i] == name {
                return (i, true);
            }
        }
        tables_names.push(name.to_string());
        (len, false)
    }

    pub fn create_in_memory_table<I: Index<BinKey, BinValue> + 'static>(
        storage: Arc<Storage>,
        name: String,
        index: I,
        is_it_logging: bool,
        scheme: Scheme,
        user_scheme: &[u8]
    ) -> usize {
        let mut lock = storage.tables_names.write().unwrap();
        let (number, is_exist) = Self::insert_table_name_and_get_number(&mut lock, &name);
        if is_exist {
            return number;
        }
        let table = InMemoryTable::new(
            number as u16,
            index,
            name.clone(),
            is_it_logging,
            storage.number_of_dumps.clone(),
            scheme,
            Box::from(user_scheme)
        );
        unsafe {
            (*storage.tables.get()).push(Box::new(table));
        }

        drop(lock);

        return number;
    }

    fn write_in_memory_table_on_disk(storage: Arc<Storage>, name: &str, number: usize, is_it_logging: bool, user_scheme: &[u8]) {
        let name_len = name.len();
        let mut buf = Vec::with_capacity(8 + name_len + user_scheme.len());
        buf.extend_from_slice(&[CREATE_TABLE_IN_MEMORY, number as u8, (number >> 8) as u8, name_len as u8, (name_len >> 8) as u8]);
        buf.extend_from_slice(name.as_bytes());
        // TODO: index from log
        let is_it_logging_byte = if is_it_logging { 1 } else { 0 };
        buf.extend_from_slice(&[is_it_logging_byte]);
        buf.extend_from_slice(&[user_scheme.len() as u8, (user_scheme.len() >> 8) as u8]);
        buf.extend_from_slice(user_scheme);
        Self::write_table_config_on_disk(storage.clone(), &buf);
    }

    pub fn create_on_disk_table<I: Index<BinKey, (u64, u64)> + 'static>(
        storage: Arc<Storage>,
        name: String,
        index: I,
        scheme: Scheme,
        user_scheme: &[u8]
    ) -> usize {
        let mut lock = storage.tables_names.write().unwrap();
        let (number, is_exist) = Self::insert_table_name_and_get_number(&mut lock, &name);
        if is_exist {
            return number;
        }
        let table = OnDiskTable::new(
            name.clone(),
            512,
            index,
            scheme,
            Box::from(user_scheme)
        );
        unsafe {
            (*storage.tables.get()).push(Box::new(table));
        }

        drop(lock);

        return number;
    }

    fn write_on_disk_table_on_disk(storage: Arc<Storage>, name: &str, number: usize, user_scheme: &[u8]) {
        let name_len = name.len();
        let mut buf = Vec::with_capacity(7 + name_len + user_scheme.len());
        buf.extend_from_slice(&[CREATE_TABLE_ON_DISK, number as u8, (number >> 8) as u8, name_len as u8, (name_len >> 8) as u8]);
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&[user_scheme.len() as u8, (user_scheme.len() >> 8) as u8]);
        buf.extend_from_slice(user_scheme);
        // TODO: index from log
        Self::write_table_config_on_disk(storage.clone(), &buf);
    }

    pub fn create_cache_table<I: Index<BinKey, (u64, BinValue)> + 'static>(
        storage: Arc<Storage>,
        name: String,
        index: I,
        cache_duration: u64,
        is_it_logging: bool,
        scheme: Scheme,
        user_scheme: &[u8]
    ) -> usize {
        let mut lock = storage.tables_names.write().unwrap();
        let (number, is_exist) = Self::insert_table_name_and_get_number(&mut lock, &name);
        if is_exist {
            return number;
        }
        let table = CacheTable::new(
            number as u16,
            index,
            cache_duration,
            name.clone(),
            is_it_logging,
            storage.number_of_dumps.clone(),
            scheme,
            Box::from(user_scheme),
        );
        unsafe {
            (*storage.tables.get()).push(Box::new(table));
        }
        storage.cache_tables_indexes.write().unwrap().push(number);

        drop(lock);

        return number;
    }

    fn write_cache_table_on_disk(
        storage: Arc<Storage>,
        name: &str,
        number: usize,
        is_it_logging: bool,
        cache_duration: u64,
        user_scheme: &[u8],
    ) {
        let name_len = name.len();
        let mut buf = Vec::with_capacity(16 + name_len + user_scheme.len());
        buf.extend_from_slice(&[CREATE_TABLE_CACHE, number as u8, (number >> 8) as u8, name_len as u8, (name_len >> 8) as u8]);
        buf.extend_from_slice(name.as_bytes());
        // TODO: index from log
        let is_it_logging_byte = if is_it_logging { 1 } else { 0 };
        buf.extend_from_slice(&[is_it_logging_byte]);
        buf.extend_from_slice(&uint::u64tob(cache_duration));
        buf.extend_from_slice(&[user_scheme.len() as u8, (user_scheme.len() >> 8) as u8]);
        buf.extend_from_slice(user_scheme);
        Self::write_table_config_on_disk(storage.clone(), &buf);
    }

    fn get_log_file_number(path: PathBuf) -> usize {
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

    pub fn rise(storage: Arc<Self>) {
        #[inline(always)]
        fn read_more(buf: &mut [u8], start_offset: usize, bytes_read: usize, offset_last_record: &mut usize) {
            let slice_to_copy = &mut Vec::with_capacity(0);
            buf[start_offset..bytes_read].clone_into(slice_to_copy);
            *offset_last_record = bytes_read - start_offset;
            buf[0..*offset_last_record].copy_from_slice(slice_to_copy);
        }
        let file_ = File::open(storage.table_configs_file_path.clone());
        if file_.is_ok() {
            let mut file = file_.unwrap();

            let mut buf = [0u8; 4096];
            let mut offset;
            let mut offset_last_record = 0;
            let mut start_offset = 0;
            let mut number;
            let mut cache_duration;
            let mut name_len;
            let mut name_offset;
            let mut is_it_logging;
            let mut name;
            let mut total_read = 0;
            let mut table_engine;
            let file_len = file.metadata().unwrap().len();
            let mut scheme_offset;
            let mut scheme_len;
            let mut total_tables = 0;
            'read: loop {
                if unlikely(total_read == file_len) {
                    break;
                }
                let mut bytes_read = file.read(&mut buf[offset_last_record..]).expect("Failed to read");
                if unlikely(bytes_read == 0) {
                    break;
                }


                bytes_read += offset_last_record;
                offset = 0;
                total_read += bytes_read as u64;

                loop {
                    if unlikely(offset + 1 > bytes_read) {
                        read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                        continue 'read;
                    }
                    start_offset = offset;
                    table_engine = buf[offset];
                    offset += 1;
                    match table_engine {
                        CREATE_TABLE_IN_MEMORY => {
                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            // TODO: think about safe of pushing
                            number = (buf[offset + 1] as u16) << 8 | (buf[offset] as u16);
                            offset += 2;

                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            name_len = (buf[offset + 1] as u32) << 8 | (buf[offset] as u32);
                            offset += 2;

                            if unlikely(offset + name_len as usize > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            name_offset = offset;
                            offset += name_len as usize;
                            name = vec![0; name_len as usize];
                            name.copy_from_slice(&buf[name_offset..offset]);
                            let name = String::from_utf8(name).unwrap();

                            if unlikely(offset + 1 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            is_it_logging = buf[offset] != 0;
                            offset += 1;

                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            scheme_len = (buf[offset + 1] as u16) << 8 | (buf[offset] as u16);
                            offset += 2;
                            scheme_offset = offset;

                            if unlikely(offset + scheme_len as usize > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            offset += scheme_len as usize;

                            let user_scheme: &[u8];
                            let scheme;
                            if scheme_len == 0 {
                                user_scheme = &[];
                                scheme = Ok(empty_scheme());
                            } else {
                                user_scheme = &buf[scheme_offset..offset];
                                scheme = scheme_from_bytes(user_scheme);
                                if scheme.is_err() {
                                    continue;
                                }
                            }

                            Self::create_in_memory_table(storage.clone(), name, HashInMemoryIndex::new(), is_it_logging, scheme.unwrap(), user_scheme);
                        }
                        CREATE_TABLE_ON_DISK => {
                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            // TODO: think about safe of pushing
                            number = (buf[offset + 1] as u16) << 8 | (buf[offset] as u16);
                            offset += 2;

                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            name_len = (buf[offset + 1] as u32) << 8 | (buf[offset] as u32);
                            offset += 2;

                            if unlikely(offset + name_len as usize > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            name_offset = offset;
                            offset += name_len as usize;
                            name = vec![0; name_len as usize];
                            name.copy_from_slice(&buf[name_offset..offset]);
                            let name = String::from_utf8(name).unwrap();

                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            scheme_len = (buf[offset + 1] as u16) << 8 | (buf[offset] as u16);
                            offset += 2;
                            scheme_offset = offset;

                            if unlikely(offset + scheme_len as usize > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            offset += scheme_len as usize;

                            let user_scheme: &[u8];
                            let scheme;
                            if scheme_len == 0 {
                                user_scheme = &[];
                                scheme = Ok(empty_scheme());
                            } else {
                                user_scheme = &buf[scheme_offset..offset];
                                scheme = scheme_from_bytes(user_scheme);
                                if scheme.is_err() {
                                    continue;
                                }
                            }

                            Self::create_on_disk_table(storage.clone(), name, HashInMemoryIndex::new(), scheme.unwrap(), user_scheme);
                        }
                        CREATE_TABLE_CACHE => {
                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            // TODO: think about safe of pushing
                            number = (buf[offset + 1] as u16) << 8 | (buf[offset] as u16);
                            offset += 2;

                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            name_len = (buf[offset + 1] as u32) << 8 | (buf[offset] as u32);
                            offset += 2;

                            if unlikely(offset + name_len as usize > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            name_offset = offset;
                            offset += name_len as usize;
                            name = vec![0; name_len as usize];
                            name.copy_from_slice(&buf[name_offset..offset]);
                            let name = String::from_utf8(name).unwrap();

                            if unlikely(offset + 1 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            is_it_logging = buf[offset] != 0;
                            offset += 1;

                            if unlikely(offset + 8 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            cache_duration = (buf[offset + 7] as u64) << 56 | (buf[offset + 6] as u64) << 48 | (buf[offset + 5] as u64) << 40 | (buf[offset + 4] as u64) << 32 | (buf[offset + 3] as u64) << 24 | (buf[offset + 2] as u64) << 16 | (buf[offset + 1] as u64) << 8 | (buf[offset] as u64);
                            offset += 8;

                            if unlikely(offset + 2 > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }

                            scheme_len = (buf[offset + 1] as u16) << 8 | (buf[offset] as u16);
                            offset += 2;
                            scheme_offset = offset;

                            if unlikely(offset + scheme_len as usize > bytes_read) {
                                read_more(&mut buf, start_offset, bytes_read, &mut offset_last_record);
                                continue 'read;
                            }
                            offset += scheme_len as usize;

                            let user_scheme: &[u8];
                            let scheme;
                            if scheme_len == 0 {
                                user_scheme = &[];
                                scheme = Ok(empty_scheme());
                            } else {
                                user_scheme = &buf[scheme_offset..offset];
                                scheme = scheme_from_bytes(user_scheme);
                                if scheme.is_err() {
                                    continue;
                                }
                            }

                            Self::create_cache_table(storage.clone(), name, HashInMemoryIndex::new(), cache_duration, is_it_logging, scheme.unwrap(), user_scheme);
                        }
                        _ => {
                            panic!("Unknown engine: {}", table_engine);
                        }
                    }
                    total_tables += 1;
                }
            }
            storage.last_tables_count.store(total_tables, SeqCst);
        }

        let storage_for_rise = storage.clone();
        let mut tables;
        unsafe {
            tables = (&mut *storage_for_rise.tables.get());
        }
        println!("{} {:?} {}", tables.len(), storage.table_configs_file_path.clone(), File::open(storage.table_configs_file_path.clone()).unwrap().metadata().unwrap().len());
        let mut joins = Vec::with_capacity((tables).len());
        for table in tables.iter_mut() {
            unsafe {
                let table_ptr = std::mem::transmute::<&mut Box<dyn Table>, &'static mut Box<dyn Table>>(table);
                joins.push(thread::spawn(move || {
                    table_ptr.rise();
                }));
            }
        }
        for join in joins {
            join.join().unwrap();
        }

        Self::read_log(storage.clone());
    }

    pub fn read_log(storage: Arc<Self>) {
        #[inline(always)]
        fn read_more(chunk: &mut [u8], start_offset: usize, bytes_read: usize, offset_last_record: &mut usize) {
            let slice_to_copy = &mut Vec::with_capacity(0);
            chunk[start_offset..bytes_read].clone_into(slice_to_copy);
            *offset_last_record = bytes_read - start_offset;
            chunk[0..*offset_last_record].copy_from_slice(slice_to_copy);
        }

        let log_number = Self::get_log_file_number(storage.number_of_dumps_file_path.clone());

        let file_name = format!("log{}.bin", log_number);
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
        let mut scheme_len;
        let mut scheme_offset;
        let tables;
        unsafe {
            tables = (&mut *storage.tables.get()) as &mut Vec<Box<dyn Table>>;
        }

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
                if action == 4 {
                    panic!();
                }
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
                    CREATE_TABLE_IN_MEMORY => {
                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        // TODO: think about safe of pushing
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
                        let name = String::from_utf8(name).unwrap();

                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        scheme_len = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;
                        scheme_offset = offset;

                        if unlikely(offset + scheme_len as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        offset += scheme_len as usize;

                        let user_scheme: &[u8];
                        let scheme;
                        if scheme_len == 0 {
                            user_scheme = &[];
                            scheme = Ok(empty_scheme());
                        } else {
                            user_scheme = &chunk[scheme_offset..offset];
                            scheme = scheme_from_bytes(user_scheme);
                            if scheme.is_err() {
                                continue;
                            }
                        }

                        Self::create_in_memory_table(storage.clone(), name, HashInMemoryIndex::new(), is_it_logging, scheme.unwrap(), user_scheme);
                    }
                    CREATE_TABLE_ON_DISK => {
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
                        let name = String::from_utf8(name).unwrap();

                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        scheme_len = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;
                        scheme_offset = offset;

                        if unlikely(offset + scheme_len as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        offset += scheme_len as usize;

                        let user_scheme: &[u8];
                        let scheme;
                        if scheme_len == 0 {
                            user_scheme = &[];
                            scheme = Ok(empty_scheme());
                        } else {
                            user_scheme = &chunk[scheme_offset..offset];
                            scheme = scheme_from_bytes(user_scheme);
                            if scheme.is_err() {
                                continue;
                            }
                        }

                        Self::create_on_disk_table(storage.clone(), name, HashInMemoryIndex::new(), scheme.unwrap(), user_scheme);
                    }
                    CREATE_TABLE_CACHE => {
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
                        let name = String::from_utf8(name).unwrap();

                        if unlikely(offset + 2 > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }

                        scheme_len = (chunk[offset + 1] as u16) << 8 | (chunk[offset] as u16);
                        offset += 2;
                        scheme_offset = offset;

                        if unlikely(offset + scheme_len as usize > bytes_read) {
                            read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                            continue 'read;
                        }
                        offset += scheme_len as usize;

                        let user_scheme: &[u8];
                        let scheme;
                        if scheme_len == 0 {
                            user_scheme = &[];
                            scheme = Ok(empty_scheme());
                        } else {
                            user_scheme = &chunk[scheme_offset..offset];
                            scheme = scheme_from_bytes(user_scheme);
                            if scheme.is_err() {
                                continue;
                            }
                        }

                        Self::create_cache_table(storage.clone(), name, HashInMemoryIndex::new(), cache_duration, is_it_logging, scheme.unwrap(), user_scheme);
                    }
                    _ => {
                        panic!("Unknown action was detected while reading the log: {}", action);
                    }
                };
            }
        }
    }
}