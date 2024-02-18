use std::{
    fs::{File, metadata},
    hash::{BuildHasher, Hash, Hasher},
    io::{Read},
    sync:: {
        {Arc, RwLock, Mutex},
        atomic::AtomicU64
    }
};
use std::path::PathBuf;
use ahash::{HashMap, HashMapExt, RandomState};
use positioned_io::{ReadAt};
use crate::bin_types::{BinKey, BinValue};
use crate::index::Index;
use crate::writers::{get_size_for_key_len, get_size_for_value_len, SizedWriter};

const BUFFER_SIZE: usize = 4100;
const DELETE_BUFFER_SIZE: usize = 66;

pub struct DiskStorage<I: Index<BinKey, (u64, u64)>> {
    /// Here `Vec<u8>` is the key.
    /// Be careful! Size and offset to the VALUE, not to the value and key and 6 bytes for the size of the value and key.
    /// You can think, that we can use a struct instead. We can't, it is make this code too slow.
    pub(crate) infos: I,
    path: PathBuf,
    atomic_indexes: Box<[Arc<AtomicU64>]>,
    files: Box<[Arc<Mutex<SizedWriter<File>>>]>,
    read_files: Box<[Arc<RwLock<File>>]>,
    files_for_need_to_delete: Box<[Arc<Mutex<SizedWriter<File>>>]>,
    size: usize,
    lob: usize,
    rs: RandomState
}

// CRUD
impl<I: Index<BinKey, (u64, u64)>> DiskStorage<I> {
    #[inline(always)]
    pub(crate) fn insert(&self, key: BinKey, value: BinValue) -> bool {
        if self.infos.contains(&key) {
            return false;
        }

        let (file, atomic_index) = self.get_file(&key);

        let kl = key.len();
        let k_size = get_size_for_key_len(kl);
        let vl = value.len();
        let v_size = get_size_for_value_len(vl);
        let index;
        {
            let mut file = file.lock().unwrap();
            file.write_key_with_size(&key, k_size).expect("failed to write to file");
            file.write_value_with_size(&value, v_size).expect("failed to write to file");
            file.flush().expect("failed to flush");
            index = atomic_index.fetch_add((vl + v_size) as u64, std::sync::atomic::Ordering::SeqCst);
        }

        // TODO: should we not to use usize in indexes?
        self.infos.insert(key, (vl as u64, index + (k_size + kl) as u64));
        true
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &BinKey) -> Option<BinValue>{
        // TODO: uncomment
        // TODO: should we use BufReader?
        let (file, info) = self.get_index_and_file(key)?;

        let mut buf = vec![0; info.0 as usize];
        file.read().unwrap().read_at(info.1, &mut buf).expect("failed to read");

        return Some(BinValue::new(buf.as_slice()));
    }

    #[inline(always)]
    pub(crate) fn delete(&self, key: &BinKey) {
        let file_lock = self.get_need_to_delete(key);
        let mut file = file_lock.lock().unwrap();
        if self.infos.remove(key).is_none() {
            return;
        }

        file.write_key(key).expect("failed to write");
        file.flush().expect("failed to flush");
    }

    #[inline(always)]
    pub(crate) fn set(&self, key: BinKey, value: BinValue) -> Option<BinValue> {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.lob;
        let file = self.files[number].clone();
        let atomic_index = self.atomic_indexes[number].clone();

        let kl = key.len();
        let size_kl= get_size_for_key_len(kl);
        let vl = value.len();
        let size_vl = get_size_for_value_len(vl);
        let index;
        {
            let mut file = file.lock().unwrap();
            file.write_key_with_size(&key, size_kl).expect("failed to write");
            file.flush().expect("failed to flush");
            index = atomic_index.fetch_add((vl + size_vl) as u64, std::sync::atomic::Ordering::SeqCst);
        }

        let old_value = self.infos.set(key.clone(), (vl as u64, index + (size_kl + kl) as u64));

        if old_value.is_some() {
            let delete_file_ = self.files_for_need_to_delete[number].clone();
            let mut delete_file = delete_file_.lock().unwrap();
            delete_file.write_key_with_size(&key, size_kl).expect("failed to write");
            delete_file.flush().expect("failed to flush");

            let info = unsafe { old_value.unwrap_unchecked() };
            let mut buf = vec![0; info.0 as usize];
            let file = self.read_files[number].clone();
            // TODO: should we use BufReader?
            file.read().unwrap().read_at(info.1, &mut buf).expect("failed to read");

            return Some(BinValue::new(buf.as_slice()));
        } else {
            return None;
        }
    }
}

// Persistence
impl<I: Index<BinKey, (u64, u64)>> DiskStorage<I> {
    // TODO: test it, because I get error in mutexes above. Maybe I didn't wait for it, but check it.
    pub fn rise(&mut self) -> bool {
        let path = self.path.clone();
        // check for the existence of the directory
        if !metadata(path.clone()).is_ok() {
            return false;
        }

        let mut files = Vec::with_capacity(self.size);
        let mut read_files = Vec::with_capacity(self.size);
        let mut files_for_need_to_delete = Vec::with_capacity(self.size);
        let mut atomic_indexes = Vec::with_capacity(self.size);

        for i in 0..self.size {
            files.push(Arc::new(Mutex::new(SizedWriter::new_with_capacity(File::open(format!("{:?}/{}", path.clone(), i)).unwrap(), BUFFER_SIZE))));
            read_files.push(Arc::new(RwLock::new(File::open(format!("{:?}/{}", path.clone(), i)).unwrap())));
            files_for_need_to_delete.push(Arc::new(Mutex::new(SizedWriter::new_with_capacity(File::open(format!("{:?}/{}D", path.clone(), i)).unwrap(), DELETE_BUFFER_SIZE))));
            atomic_indexes.push(Arc::new(AtomicU64::new(0)));
        }

        self.files = files.into_boxed_slice();
        self.read_files = read_files.into_boxed_slice();
        self.files_for_need_to_delete = files_for_need_to_delete.into_boxed_slice();
        self.atomic_indexes = atomic_indexes.into_boxed_slice();

        let mut chunk = vec![0u8; 1024 * 1024];
        let mut read = 0;
        let mut file_len;

        let mut offset_last_record = 0;
        let mut offset;
        let mut start_offset = 0;
        let mut key_offset;

        let mut tmp_set = HashMap::with_capacity(2<<16);

        for i in 0..self.size {
            let lock = self.files_for_need_to_delete[i].lock().unwrap();
            let mut file = lock.inner.get_ref();
            file_len = file.metadata().unwrap().len();
            'read_loop: loop {
                if read == file_len {
                    break;
                }

                let mut bytes_read = file.read(&mut chunk[offset_last_record..]).expect("Failed to read");
                if bytes_read == 0 {
                    break;
                }

                bytes_read += offset_last_record;
                offset = 0;
                read += bytes_read as u64;

                loop {
                    if offset + 1 > bytes_read {
                        let slice_to_copy = &mut Vec::with_capacity(0);
                        chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                        offset_last_record = bytes_read - start_offset;
                        chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                        continue 'read_loop;
                    }
                    start_offset = offset;
                    let mut kl = chunk[offset + 1] as u32;
                    if kl < 255 {
                        offset += 1;
                    } else {
                        kl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        if offset + 3 > bytes_read {
                            let slice_to_copy = &mut Vec::with_capacity(0);
                            chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                            offset_last_record = bytes_read - start_offset;
                            chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                            continue 'read_loop;
                        }
                        offset += 3;
                    }

                    if offset + kl as usize > bytes_read {
                        let slice_to_copy = &mut Vec::with_capacity(0);
                        chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                        offset_last_record = bytes_read - start_offset;
                        chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                        continue 'read_loop;
                    }
                    key_offset = offset;
                    offset += kl as usize;

                    let key = chunk[key_offset..key_offset+kl as usize].to_vec();
                    tmp_set.entry(key).and_modify(|value| {
                        *value += 1;
                    }).or_insert(1);
                }
            }
            offset_last_record = 0;
            start_offset = 0;
            read = 0;
            let lock = self.files[i].lock().unwrap();
            let mut file = lock.inner.get_ref();
            file_len = file.metadata().unwrap().len();
            'read_loop: loop {
                if read == file_len {
                    break;
                }

                let mut bytes_read = file.read(&mut chunk[offset_last_record..]).expect("Failed to read");
                if bytes_read == 0 {
                    break;
                }

                bytes_read += offset_last_record;
                offset = 0;
                read += bytes_read as u64;

                loop {
                    if offset + 1 > bytes_read {
                        let slice_to_copy = &mut Vec::with_capacity(0);
                        chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                        offset_last_record = bytes_read - start_offset;
                        chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                        continue 'read_loop;
                    }
                    start_offset = offset;
                    let mut kl = chunk[offset + 1] as u32;
                    if kl < 255 {
                        offset += 1;
                    } else {
                        if offset + 3 > bytes_read {
                            let slice_to_copy = &mut Vec::with_capacity(0);
                            chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                            offset_last_record = bytes_read - start_offset;
                            chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                            continue 'read_loop;
                        }
                        kl = (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                        offset += 3;
                    }

                    if offset + kl as usize + 3 /*3 here is bytes for vl*/ > bytes_read {
                        let slice_to_copy = &mut Vec::with_capacity(0);
                        chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                        offset_last_record = bytes_read - start_offset;
                        chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                        continue 'read_loop;
                    }
                    key_offset = offset;
                    offset += kl as usize;

                    let vl = (chunk[offset + 2] as u32) << 16 | (chunk[offset + 1] as u32) << 8 | (chunk[offset] as u32);
                    offset += 3;

                    if offset + vl as usize > bytes_read {
                        let slice_to_copy = &mut Vec::with_capacity(0);
                        chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                        offset_last_record = bytes_read - start_offset;
                        chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                        continue 'read_loop;
                    }

                    offset += vl as usize;

                    let key = chunk[key_offset..key_offset+kl as usize].to_vec();
                    let mut hasher = self.rs.build_hasher();
                    key.hash(&mut hasher);
                    let number = hasher.finish() as usize & self.lob;

                    unsafe {
                        let atomic = self.atomic_indexes.get_unchecked(number);
                        let res = tmp_set.get_mut(&chunk[key_offset..key_offset+kl as usize]);
                        if res.is_some() {
                            let number = res.unwrap();
                            if *number != 0 {
                                *number -= 1;
                                atomic.fetch_add((6 + vl + kl) as u64, std::sync::atomic::Ordering::SeqCst);
                                continue;
                            }
                        }
                        self.infos.insert(BinKey::new(key.as_slice()), (vl as u64, atomic.fetch_add((6 + vl + kl) as u64, std::sync::atomic::Ordering::SeqCst) + 6 +(kl as u64)));
                    }
                }
            }
            offset_last_record = 0;
            start_offset = 0;
            read = 0;
            tmp_set.clear();
        }

        return true;
    }
}

// some helpers function
impl<I: Index<BinKey, (u64, u64)>> DiskStorage<I> {
    #[allow(unused_variables)]
    pub(crate) fn new(path: PathBuf, size: usize, index: I) -> DiskStorage<I> {
        #[cfg(target_os = "windows")] {
            panic!("Do not use windows for `on disk` storage. It is not implemented yet.");
        }
        #[cfg(not(target_os = "windows"))] {
            let size = {
                if size.is_power_of_two() {
                    size
                } else {
                    size.next_power_of_two()
                }
            };
            let mask = f64::log2(size as f64) as u32;
            let lob = (1 << mask) - 1;
            let rs = RandomState::with_seeds(123412341234, 1234123412343214, 12342134, 12341234123);

            let mut storage = Self {
                infos: index,
                atomic_indexes: vec![].into_boxed_slice(),
                files: vec![].into_boxed_slice(),
                read_files: vec![].into_boxed_slice(),
                files_for_need_to_delete: vec![].into_boxed_slice(),
                path,
                size,
                lob,
                rs,
            };

            let does_exist = storage.rise();
            if does_exist {
                return storage;
            }

            let mut files = Vec::with_capacity(size);
            let mut read_files = Vec::with_capacity(size);
            let mut files_for_need_to_delete = Vec::with_capacity(size);
            let mut atomic_indexes = Vec::with_capacity(size);
            std::fs::DirBuilder::new().create(path.clone()).unwrap();
            for i in 0..size {
                files.push(Arc::new(Mutex::new(SizedWriter::new_with_capacity(File::create(format!("{:?}/{}", path.clone(), i)).unwrap(), BUFFER_SIZE))));
                read_files.push(Arc::new(RwLock::new(File::open(format!("{:?}/{}", path.clone(), i)).unwrap())));
                files_for_need_to_delete.push(Arc::new(Mutex::new(SizedWriter::new_with_capacity(File::create(format!("{:?}/{}D", path.clone(), i)).unwrap(), DELETE_BUFFER_SIZE))));
                atomic_indexes.push(Arc::new(AtomicU64::new(0)));
            }

            storage.files = files.into_boxed_slice();
            storage.read_files = read_files.into_boxed_slice();
            storage.files_for_need_to_delete = files_for_need_to_delete.into_boxed_slice();
            storage.atomic_indexes = atomic_indexes.into_boxed_slice();

            return storage;
        }
    }

    #[inline(always)]
    fn get_file(&self, key: &BinKey) -> (Arc<Mutex<SizedWriter<File>>>, Arc<AtomicU64>) {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.lob;
        return (self.files[number].clone(), self.atomic_indexes[number].clone());
    }

    #[inline(always)]
    fn get_index_and_file(&self, key: &BinKey) -> Option<(Arc<RwLock<File>>, (u64, u64))> {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.lob;
        let info;
        {
            let index_ = self.infos.get(key);
            if index_.is_none() {
                return None;
            }
            info = unsafe { index_.unwrap_unchecked() };
        }

        return Some((self.read_files[number].clone(), info));
    }

    #[inline(always)]
    fn get_need_to_delete(&self, key: &BinKey) -> Arc<Mutex<SizedWriter<File>>> {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.lob;
        return self.files_for_need_to_delete[number].clone();
    }
}