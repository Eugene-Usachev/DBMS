use std::fs::{DirBuilder, File};
use std::intrinsics::unlikely;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use crate::bin_types::{BinKey, BinValue};
use crate::constants;
use crate::constants::actions;
use crate::table::table::Table;
use crate::storage::storage::NOW_MINUTES;
use crate::index::Index;
use crate::utils::fastbytes::uint;
use crate::writers::{LogWriter, SizedWriter, write_to_log_with_key, write_to_log_with_key_and_value};

pub struct CacheTable<I: Index<BinKey, (u64, BinValue)>> {
    index: I,
    number: u16,
    cache_duration: u64,
    number_of_dumps: AtomicU32,
    name: String,
    is_it_logging: bool
}

impl<I: Index<BinKey, (u64, BinValue)>> CacheTable<I> {
    pub fn new(number: u16, index: I, cache_duration: u64, name: String, is_it_logging: bool, number_of_dumps: u32) -> CacheTable<I> {
        CacheTable {
            number,
            index,
            cache_duration,
            number_of_dumps: AtomicU32::new(number_of_dumps),
            name,
            is_it_logging
        }
    }
}

impl<I: Index<BinKey, (u64, BinValue)>> Table for CacheTable<I> {
    #[inline(always)]
    fn get(&self, key: &BinKey) -> Option<BinValue> {
        match self.index.get(key) {
            Some(value) => Some(value.1),
            None => None,
        }
    }

    #[inline(always)]
    fn get_and_reset_cache_time(&self, key: &BinKey) -> Option<BinValue> {
        let res = self.index.get_and_modify(key, |value| {
            value.0 = NOW_MINUTES.load(SeqCst);
        });

        if unlikely(res.is_none()) {
            return None;
        }
        Some(res.unwrap().1)
    }

    #[inline(always)]
    fn set(&self, key: BinKey, value: BinValue, log_buf: &mut [u8], log_offset: &mut usize) -> Option<BinValue> {
        if self.is_it_logging {
            write_to_log_with_key_and_value(log_buf, log_offset, actions::SET, self.number, &key, &value);
        }

        let res = self.index.set(key, (NOW_MINUTES.load(SeqCst), value));
        if unlikely(res.is_none()) {
            return None;
        }
        Some(res.unwrap().1)
    }

    #[inline(always)]
    fn set_without_log(&self, key: BinKey, value: BinValue) -> Option<BinValue> {
        let res = self.index.set(key, (NOW_MINUTES.load(SeqCst), value));
        if unlikely(res.is_none()) {
            return None;
        }
        Some(res.unwrap().1)
    }

    #[inline(always)]
    fn insert(&self, key: BinKey, value: BinValue, log_buf: &mut [u8], log_offset: &mut usize) -> bool {
        if self.is_it_logging {
            write_to_log_with_key_and_value(log_buf, log_offset, actions::INSERT, self.number, &key, &value);
        }

        self.index.insert(key, (NOW_MINUTES.load(SeqCst), value))
    }

    #[inline(always)]
    fn insert_without_log(&self, key: BinKey, value: BinValue) -> bool {
        self.index.insert(key, (NOW_MINUTES.load(SeqCst), value))
    }

    #[inline(always)]
    fn delete(&self, key: &BinKey, log_buf: &mut [u8], log_offset: &mut usize) {
        if self.is_it_logging {
            write_to_log_with_key(log_buf, log_offset, actions::DELETE, self.number, key);
        }

        self.index.remove(key);
    }

    #[inline(always)]
    fn delete_without_log(&self, key: &BinKey) {
        self.index.remove(key);
    }

    #[inline(always)]
    fn count(&self) -> u64 {
        self.index.count() as u64
    }

    #[inline(always)]
    fn invalid_cache(&self) {
        let now = NOW_MINUTES.load(SeqCst);
        let duration = self.cache_duration;

        self.index.retain(|_, value| {
            return value.0 + duration > now;
        });
    }

    fn dump(&self) {
        const BUF_SIZE: usize = 64 * 1024;
        const BUF_SIZE_MAX: usize = BUF_SIZE - 1;
        const COUNT_OF_ELEMS_SIZE: usize = 8;

        let number = self.number_of_dumps.fetch_add(1, SeqCst);
        if number == 0 {
            let dir_path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &self.name].iter().collect();
            let _ = DirBuilder::new().recursive(true).create(dir_path);
        }
        let file_name = format!("{}{number}.dump", self.name);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &self.name, &file_name].iter().collect();
        // TODO: maybe remove old dumps?
        let output = File::create(path.clone()).expect(&*format!("failed to create file with path {}", path.to_string_lossy()));
        let mut writer = SizedWriter::new_with_capacity(output, BUF_SIZE);
        let mut count = 0;

        /// COUNT_OF_ELEMS_SIZE bytes for number of elements and one byte for a flag.
        /// We will set the flag to one, when we finish dumping
        writer.write(&[0u8;COUNT_OF_ELEMS_SIZE + 1]).expect("failed to write");
        self.index.for_each_mut(|key, value| {
            count += 1;
            writer.write_key(key).expect("failed to write");
            writer.write_value(&value.1).expect("failed to write");
        });
        /// Write number of elements and change the flag
        let mut buf = [1u8;COUNT_OF_ELEMS_SIZE+1];
        buf[0] = count as u8;
        buf[1..COUNT_OF_ELEMS_SIZE + 1].copy_from_slice(&uint::u64tob(count as u64));
        /// Seek will flush the buffer before seek.
        writer.inner.seek(SeekFrom::Start(0)).expect("failed to seek");
        writer.inner.write_all(&buf).expect("failed to write");
        writer.inner.flush().expect("failed to flush");
    }

    fn rise(&mut self, number_of_dumps: u32) {
        #[inline(always)]
        fn read_more(chunk: &mut [u8], start_offset: usize, bytes_read: usize, offset_last_record: &mut usize) {
            let slice_to_copy = &mut Vec::with_capacity(0);
            chunk[start_offset..bytes_read].clone_into(slice_to_copy);
            *offset_last_record = bytes_read - start_offset;
            chunk[0..*offset_last_record].copy_from_slice(slice_to_copy);
        }

        self.number_of_dumps.store(number_of_dumps, SeqCst);
        let file_name = format!("{}{}.dump", self.name, number_of_dumps - 1);
        let path: PathBuf = ["..", constants::paths::PERSISTENCE_DIR, &self.name, &file_name].iter().collect();

        let mut input = File::open(path.clone()).expect(&*format!("Failed to open file with path: {}", path.to_string_lossy()));
        let file_len = input.metadata().unwrap().len();
        if unlikely(file_len < 8) {
            panic!("file len is less than 8!");
        }
        let mut chunk = [0u8; 64 * 1024];
        input.read(&mut chunk[..8]).expect("can't get count from buf!");
        let all_count = uint::u64(&chunk[..8]);
        self.index.resize(((all_count as f64) * 1.2) as usize);
        let mut total_read = 8;
        let mut total_records_read = 0;

        let mut offset_last_record = 0;
        let mut offset;
        let mut start_offset = 0;
        let mut key_offset;
        let mut value_offset;
        let mut kl;
        let mut vl;

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

                total_records_read += 1;
                self.index.insert(BinKey::new(&chunk[key_offset..key_offset+kl as usize]),
                                  (NOW_MINUTES.load(SeqCst), BinValue::new(&chunk[value_offset..offset])));
            }
        }

        if unlikely(total_records_read != all_count) {
            println!("Bad dump read! Lost {} records in dump file with name: {}", all_count - total_records_read, file_name);
        }
    }
}

unsafe impl<I: Index<BinKey, (u64, BinValue)>> Sync for CacheTable<I> {}
unsafe impl<I: Index<BinKey, (u64, BinValue)>> Send for CacheTable<I> {}