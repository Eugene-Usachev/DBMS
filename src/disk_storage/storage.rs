#[allow(unused_imports)]
use std::{
    fs::{File, metadata, DirBuilder},
    hash::{BuildHasher, Hash, Hasher},
    io::{Read, Write},
    sync:: {
        {Arc, RwLock, Mutex},
        atomic::AtomicU64
    }
};
use ahash::{HashMap, HashMapExt, RandomState};
use dashmap::DashMap;
use positioned_io::{ReadAt};
use crate::constants::paths::PERSISTENCE_DIR;

pub struct DiskStorage {
    /// Here `Vec<u8>` is the key.
    /// Be careful! Size and offset to the VALUE, not to the value and key and 6 bytes for the size of the value and key.
    pub(crate) infos: DashMap<Vec<u8>, (u64, u64), RandomState>,
    atomic_indexes: Box<[Arc<AtomicU64>]>,
    files: Box<[Arc<Mutex<File>>]>,
    read_files: Box<[Arc<RwLock<File>>]>,
    files_for_need_to_delete: Box<[Arc<Mutex<File>>]>,
    path: String,
    size: usize,
    mask: usize,
    rs: RandomState
}

// CRUD
impl DiskStorage {
    #[inline(always)]
    pub(crate) fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let (file, atomic_index) = self.get_file(&key);

        let kl = key.len();
        let vl = value.len();
        let size = (6 + kl + vl) as u64;
        let mut buf = Vec::with_capacity(size as usize);
        buf.resize(size as usize, 0);
        let size_kl;
        if kl < 255 {
            buf[0] = kl as u8;
            size_kl = 1;
        } else {
            buf[0] = 255;
            buf[1..3].copy_from_slice(&[kl as u8, ((kl >> 8) as u8)]);
            size_kl = 3;
        }
        let mut offset = size_kl + kl;
        buf[size_kl..offset].copy_from_slice(key.as_slice());
        buf[offset..offset+3].copy_from_slice(&[vl as u8, ((vl >> 8) as u8),  (vl >> 16) as u8]);
        offset += 3;
        buf[offset..offset+vl].copy_from_slice(value.as_slice());

        let index;

        {
            let mut file = file.lock().unwrap();
            file.write_all(&buf).expect("failed to write");
            index = atomic_index.fetch_add(size, std::sync::atomic::Ordering::SeqCst);
        }

        self.infos.insert(key, (vl as u64, index+3 + (size_kl + kl) as u64));
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>>{
        let res = self.get_index_and_file(key);
        if res.is_none() {
            return None;
        }

        let (file, info) = unsafe { res.unwrap_unchecked() };
        let mut buf = vec![0; info.0 as usize];
        file.read().unwrap().read_at(info.1, &mut buf).expect("failed to read");

        return Some(buf);
    }

    #[inline(always)]
    pub(crate) fn delete(&self, key: &Vec<u8>) {
        let file = self.get_need_to_delete(key);
        if self.infos.remove(key).is_none() {
            return;
        }

        let kl = key.len();
        let size_kl;
        if kl < 255 {
            size_kl = 1;
        } else {
            size_kl = 3;
        }

        let mut buf = Vec::with_capacity(size_kl+kl);
        buf.resize(size_kl+kl, 0);
        if kl < 255 {
            buf[0] = kl as u8;
        } else {
            buf[0] = 255;
            buf[1..3].copy_from_slice(&[kl as u8, ((kl >> 8) as u8)]);
        }
        buf[size_kl..size_kl+kl].copy_from_slice(key.as_slice());

        file.lock().unwrap().write_all(&buf).expect("failed to write");
    }

    #[inline(always)]
    pub(crate) fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.mask;
        let file = self.files[number].clone();
        let atomic_index = self.atomic_indexes[number].clone();

        let kl = key.len();
        let vl = value.len();
        let size_kl;
        if kl < 255 {
            size_kl = 1;
        } else {
            size_kl = 3;
        }
        let size = (3 + size_kl + kl + vl) as u64;
        let mut buf = Vec::with_capacity(size as usize);
        buf.resize(size as usize, 0);
        if kl < 255 {
            buf[0] = kl as u8;
        } else {
            buf[0] = 255;
            buf[1..3].copy_from_slice(&[kl as u8, ((kl >> 8) as u8)]);
        }
        let mut offset = size_kl + kl;
        buf[size_kl..offset].copy_from_slice(key.as_slice());
        buf[offset..offset+3].copy_from_slice(&[vl as u8, ((vl >> 8) as u8),  (vl >> 16) as u8]);
        offset += 3;
        buf[offset..offset+vl].copy_from_slice(value.as_slice());
        let index;
        {
            let mut file = file.lock().unwrap();
            file.write_all(&buf).expect("failed to write");
            index = atomic_index.fetch_add(size, std::sync::atomic::Ordering::SeqCst);
        }

        if self.infos.insert(key, (vl as u64, index + 3 + (size_kl + kl) as u64)).is_some() {
            let delete_file_ = self.files_for_need_to_delete[number].clone();
            let mut delete_file = delete_file_.lock().unwrap();
            delete_file.write_all(&buf[..3+kl]).expect("failed to write");
        }
    }
}

// Persistence
impl DiskStorage {
    pub fn rise(&mut self) -> bool {
        let path = format!("{}/{}", PERSISTENCE_DIR, self.path.clone());
        // check for the existence of the directory
        if !metadata(path.clone()).is_ok() {
            return false;
        }

        println!("{} size: {}, path: {}", path.clone(), self.size, self.path);

        let mut files = Vec::with_capacity(self.size);
        let mut read_files = Vec::with_capacity(self.size);
        let mut files_for_need_to_delete = Vec::with_capacity(self.size);
        let mut atomic_indexes = Vec::with_capacity(self.size);

        for i in 0..self.size {
            files.push(Arc::new(Mutex::new(File::open(format!("{}/{}", path.clone(), i)).unwrap())));
            read_files.push(Arc::new(RwLock::new(File::open(format!("{}/{}", path.clone(), i)).unwrap())));
            files_for_need_to_delete.push(Arc::new(Mutex::new(File::open(format!("{}/{}D", path.clone(), i)).unwrap())));
            atomic_indexes.push(Arc::new(AtomicU64::new(0)));
        }

        self.files = files.into_boxed_slice();
        self.read_files = read_files.into_boxed_slice();
        self.files_for_need_to_delete = files_for_need_to_delete.into_boxed_slice();
        self.atomic_indexes = atomic_indexes.into_boxed_slice();

        let mut chunk = vec![0u8; 128 * 1024 * 1024];
        let mut read = 0;
        let mut file_len;

        let mut offset_last_record = 0;
        let mut offset;
        let mut start_offset = 0;
        let mut key_offset;

        let mut tmp_set = HashMap::with_capacity(2>>16);

        for i in 0..self.size {
            let mut file = self.files_for_need_to_delete[i].lock().unwrap();
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

            let mut file = self.files[i].lock().unwrap();
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
                    let number = hasher.finish() as usize & self.mask;

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
                        self.infos.insert(key, (vl as u64, atomic.fetch_add((6 + vl + kl) as u64, std::sync::atomic::Ordering::SeqCst) + 6 +(kl as u64)));
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

// some helpers functions
impl DiskStorage {
    pub(crate) fn new(path: String, size: usize) -> Self {
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
            let lob = f64::log2(size as f64) as u32;
            let mask = (1 << lob) - 1;
            let rs = RandomState::with_seeds(123412341234, 1234123412343214, 12342134, 12341234123);

            let mut storage = Self {
                infos: DashMap::with_hasher(rs.clone()),
                atomic_indexes: vec![].into_boxed_slice(),
                files: vec![].into_boxed_slice(),
                read_files: vec![].into_boxed_slice(),
                files_for_need_to_delete: vec![].into_boxed_slice(),
                path,
                size,
                mask,
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
            let path = format!("{}/{}", PERSISTENCE_DIR, storage.path.clone());
            DirBuilder::new().create(path.clone()).unwrap();
            for i in 0..size {
                files.push(Arc::new(Mutex::new(File::create(format!("{}/{}", path.clone(), i)).unwrap())));
                read_files.push(Arc::new(RwLock::new(File::open(format!("{}/{}", path.clone(), i)).unwrap())));
                files_for_need_to_delete.push(Arc::new(Mutex::new(File::create(format!("{}/{}D", path.clone(), i)).unwrap())));
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
    fn get_file(&self, key: &Vec<u8>) -> (Arc<Mutex<File>>, Arc<AtomicU64>) {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.mask;
        return (self.files[number].clone(), self.atomic_indexes[number].clone());
    }

    #[inline(always)]
    fn get_index_and_file(&self, key: &[u8]) -> Option<(Arc<RwLock<File>>, (u64, u64))> {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.mask;
        let info;
        {
            let index_ = self.infos.get(key);
            if index_.is_none() {
                return None;
            }
            info = unsafe { *index_.unwrap_unchecked() };
        }

        return Some((self.read_files[number].clone(), info));
    }

    #[inline(always)]
    fn get_need_to_delete(&self, key: &Vec<u8>) -> Arc<Mutex<File>> {
        let mut hasher = RandomState::build_hasher(&self.rs);
        key.hash(&mut hasher);
        let number = hasher.finish() as usize & self.mask;
        return self.files_for_need_to_delete[number].clone();
    }
}