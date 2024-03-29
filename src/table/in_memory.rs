use std::{
    fs::{DirBuilder, File},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering::SeqCst}},
};
use crate::{
    bin_types::{BinKey, BinValue},
    constants::actions,
    error,
    index::Index,
    scheme::scheme,
    writers::{LogWriter, SizedWriter},
    table::table::{Table, TableEngine},
    utils::{bytes::uint, read_more},
};

pub struct InMemoryTable<I: Index<BinKey, BinValue>> {
    persistence_dir_path: PathBuf,
    number: u16,
    pub index: I,
    number_of_dumps: Arc<AtomicU32>,
    /// Table will try to create a directory if it is false
    was_dumped: AtomicBool,
    name: String,
    is_it_logging: bool,
    scheme: scheme::Scheme,
    user_scheme: Box<[u8]>,
}

impl<I: Index<BinKey, BinValue>> InMemoryTable<I> {
    pub fn new(
        persistence_dir_path: PathBuf,
        number: u16,
        index: I,
        name: String,
        is_it_logging: bool,
        number_of_dumps: Arc<AtomicU32>,
        scheme: scheme::Scheme,
        user_scheme: Box<[u8]>,
    ) -> InMemoryTable<I> {

        InMemoryTable {
            persistence_dir_path,
            number,
            index,
            was_dumped: AtomicBool::new(false),
            number_of_dumps,
            name,
            is_it_logging,
            scheme,
            user_scheme,
        }
    }
}

impl<I: Index<BinKey, BinValue>> Table for InMemoryTable<I> {
    #[inline(always)]
    fn engine(&self) -> TableEngine {
        TableEngine::InMemory
    }

    #[inline(always)]
    fn name(&self) -> String {
        self.name.clone()
    }

    #[inline(always)]
    fn is_it_logging(&self) -> bool {
        self.is_it_logging
    }

    #[inline(always)]
    fn cache_duration(&self) -> u64 {
        unreachable!()
    }

    #[inline(always)]
    fn get(&self, key: &BinKey) -> Option<BinValue> {
        self.index.get(key)
    }

    #[inline(always)]
    fn set(&self, key: BinKey, value: BinValue, log_writer: &mut LogWriter) -> Option<BinValue> {
        if self.is_it_logging {
            log_writer.write_key_and_value(actions::SET, self.number, &key, &value);
        }

        self.index.set(key, value)
    }

    #[inline(always)]
    fn set_without_log(&self, key: BinKey, value: BinValue) -> Option<BinValue> {
        self.index.set(key, value)
    }

    #[inline(always)]
    fn insert(&self, key: BinKey, value: BinValue, log_writer: &mut LogWriter) -> bool {
        if self.is_it_logging {
            log_writer.write_key_and_value(actions::INSERT, self.number, &key, &value);
        }

        self.index.insert(key, value)
    }

    #[inline(always)]
    fn insert_without_log(&self, key: BinKey, value: BinValue) -> bool {
        self.index.insert(key, value)
    }

    #[inline(always)]
    fn delete(&self, key: &BinKey, log_writer: &mut LogWriter) {
        if self.is_it_logging {
            log_writer.write_key(actions::DELETE, self.number, key);
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
    fn user_scheme(&self) -> Box<[u8]> {
        self.user_scheme.clone()
    }

    #[inline(always)]
    fn scheme(&self) -> &scheme::Scheme {
        &self.scheme
    }

    fn dump(&self) {
        const BUF_SIZE: usize = 64 * 1024;
        const COUNT_OF_ELEMS_SIZE: usize = 8;

        let number = self.number_of_dumps.load(SeqCst);
        if self.was_dumped.load(SeqCst) == false {
            let dir_path: PathBuf = self.persistence_dir_path.join(self.name.clone());
            let _ = DirBuilder::new().recursive(true).create(dir_path);
            self.was_dumped.store(true, SeqCst);
        }
        let file_name = format!("{}{number}.dump", self.name);
        let path: PathBuf = self.persistence_dir_path.join(self.name.clone()).join(file_name);

        // TODO: maybe remove old dumps?
        let output = File::create(path.clone()).expect(&*format!("failed to create file with path {}", path.to_string_lossy()));
        let mut writer = SizedWriter::new_with_capacity(output, BUF_SIZE);
        let mut count = 0;

        // COUNT_OF_ELEMS_SIZE bytes for number of elements and one byte for a flag.
        // We will set the flag to one, when we finish dumping
        writer.write(&[0u8;COUNT_OF_ELEMS_SIZE + 1]).expect("failed to write");
        self.index.for_each_mut(|key, value| {
            count += 1;
            writer.write_key(key).expect("failed to write");
            writer.write_value(value).expect("failed to write");
        });
        // Write number of elements and change the flag
        let mut buf = [1u8;COUNT_OF_ELEMS_SIZE+1];
        buf[0] = count as u8;
        buf[1..COUNT_OF_ELEMS_SIZE + 1].copy_from_slice(&uint::u64tob(count as u64));
        // Seek will flush the buffer before seek.
        writer.inner.seek(SeekFrom::Start(0)).expect("failed to seek");
        writer.inner.write_all(&buf).expect("failed to write");
        writer.inner.flush().expect("failed to flush");
    }

    fn rise(&mut self) {
        let number_of_dumps = self.number_of_dumps.load(SeqCst);
        if number_of_dumps == 0 {
            return;
        }

        let file_name = format!("{}{}.dump", self.name, number_of_dumps);
        let path: PathBuf = self.persistence_dir_path.join(self.name.clone()).join(file_name.clone());

        let mut input = File::open(path.clone()).expect(&*format!("Failed to open file with path: {}", path.to_string_lossy()));
        let file_len = input.metadata().unwrap().len();
        if file_len < 8 {
            panic!("file len is less than 8!");
        }
        let mut chunk = [0u8; 64 * 1024];
        input.read(&mut chunk[..9]).expect("can't get count from buf!");
        let all_count = uint::u64(&chunk[1..9]);
        self.index.resize(((all_count as f64) * 1.2) as usize);
        let mut total_read = 9;
        let mut total_records_read = 0;

        let mut offset_last_record = 0;
        let mut offset;
        let mut start_offset = 0;
        let mut key_offset;
        let mut value_offset;
        let mut kl;
        let mut vl;

        'read: loop {
            if total_read == file_len {
                break;
            }
            let mut bytes_read = input.read(&mut chunk[offset_last_record..]).expect("Failed to read");
            if bytes_read == 0 {
                break;
            }


            bytes_read += offset_last_record;
            offset = 0;
            total_read += bytes_read as u64;

            loop {
                if offset + 1 > bytes_read {
                    read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                    continue 'read;
                }
                start_offset = offset;
                kl = chunk[offset] as u32;
                offset += 1;
                if kl == 255 {
                    if offset + 2 > bytes_read {
                        read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                        continue 'read;
                    }
                    kl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                    offset += 2;
                }

                if offset + kl as usize + 2 /*for vl*/ > bytes_read {
                    read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                    continue 'read;
                }
                key_offset = offset;
                offset += kl as usize;

                vl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                offset += 2;
                if vl == 65535 {
                    if offset + 4 > bytes_read {
                        read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                        continue 'read;
                    }
                    vl = (chunk[offset + 3] as u32) << 24 | (chunk[offset + 2] as u32) << 16 | (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                    offset += 4;
                }

                if offset + vl as usize > bytes_read {
                    read_more(&mut chunk, start_offset, bytes_read, &mut offset_last_record);
                    continue 'read;
                }

                value_offset = offset;
                offset += vl as usize;

                total_records_read += 1;
                self.index.insert(BinKey::new(&chunk[key_offset..key_offset+kl as usize]), BinValue::new(&chunk[value_offset..offset]));
            }
        }

        if total_records_read < all_count {
            error!("Bad dump read! Lost {} records in dump file with name: {}", all_count - total_records_read, file_name);
        }
    }

    // NOT EXISTS!

    fn invalid_cache(&self) {
        unreachable!()
    }
}

unsafe impl<I: Index<BinKey, BinValue>> Send for InMemoryTable<I> {}
unsafe impl<I: Index<BinKey, BinValue>> Sync for InMemoryTable<I> {}