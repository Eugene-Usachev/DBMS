use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::RwLock;
use ahash::AHashMap;
use crate::constants;
use crate::space::space::SpaceInterface;
use crate::utils::hash::get_hash::get_hash;

pub struct InMemorySpace {
    pub data: Box<[RwLock<AHashMap<Vec<u8>, Vec<u8>>>]>,
    pub size: usize,
    number_of_dumps: AtomicU32,
    name: String,
}

impl InMemorySpace {
    pub fn new(size: usize, name: String, number_of_dumps: u32) -> InMemorySpace {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(RwLock::new(AHashMap::new()));
        }

        InMemorySpace {
            data: data.into_boxed_slice(),
            size,
            number_of_dumps: AtomicU32::new(number_of_dumps),
            name
        }
    }
}

impl SpaceInterface for InMemorySpace {
    #[inline(always)]
    fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        match self.data[get_hash(key) % self.size].read().unwrap().get(key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    #[inline(always)]
    fn set(&self, key: Vec<u8>,value: Vec<u8>) {
        self.data[get_hash(&key) % self.size].write().unwrap().insert(key, value);
    }

    #[inline(always)]
    fn insert(&self,  key: Vec<u8>, value: Vec<u8>) {
        self.data[get_hash(&key) % self.size].write().unwrap().entry(key).or_insert(value);
    }

    #[inline(always)]
    fn delete(&self,  key: &Vec<u8>) {
        self.data[get_hash(key) % self.size].write().unwrap().remove(key);
    }

    #[inline(always)]
    fn count(&self) -> u64 {
        let mut count = 0;
        for i in 0..self.size {
            let mut part_of_space = self.data[i].write().unwrap();
            count += part_of_space.len();
        }
        count as u64
    }

    fn dump(&self) {
        let mut buf = Box::new([0u8; 128 * 1024*1024]);
        let mut offset = 8;
        let mut kl = 0;
        let mut vl = 0;

        let file_name = format!("{}{}.dump", self.name, self.number_of_dumps.fetch_add(1, SeqCst));
        let path: PathBuf = [constants::paths::PERSISTENCE_DIR.to_string(), file_name].iter().collect();

        let count = self.count();
        buf[0..8].copy_from_slice(&[count as u8, (count >> 8) as u8, (count >> 16) as u8, (count >> 24) as u8, (count >> 32) as u8, (count >> 40) as u8, (count >> 48) as u8, (count >> 56) as u8]);
        let mut output = File::create(path.clone()).expect(&*format!("failed to create file with path {}", path.to_string_lossy()));
        for i in 0..self.size {
            let part_of_space = self.data[i].read().unwrap();
            for (k, v) in part_of_space.iter() {
                kl = k.len();
                vl = v.len();
                if offset + vl + kl +8 > 128 * 1024 * 1024 - 1 {
                    output.write(&buf[..offset]).expect("failed to write");
                    offset = 0;
                }
                buf[offset..offset + 3].copy_from_slice(&[kl as u8, ((kl >> 8) as u8),  (kl >> 16) as u8]);
                offset += 3;
                buf[offset..offset+kl].copy_from_slice(k);
                offset += kl;
                buf[offset..offset+3].copy_from_slice(&[vl as u8, ((vl >> 8) as u8),  (vl >> 16) as u8]);
                offset += 3;
                buf[offset..offset+vl].copy_from_slice(v);
                offset += vl;
            }
        }
        if offset > 0 {
            output.write(&buf[..offset]).expect("failed to write");
        }
    }

    fn rise(&self) {
        let file_name = format!("{}{}.dump", self.name, self.number_of_dumps.fetch_add(1, SeqCst));
        let path: PathBuf = [constants::paths::PERSISTENCE_DIR.to_string(), file_name].iter().collect();

        let mut input = File::open(path.clone()).expect(&*format!("Failed to open file with path: {}", path.to_string_lossy()));
        let file_len = input.metadata().unwrap().len();
        if file_len < 8 {
            panic!("file len is less than 8!");
        }
        let mut count_buf = [0;8];
        input.read(&mut count_buf).expect("can't get count from buf!");
        let mut total_read = 8;
        let mut chunk = vec![0u8; 128 * 1024 * 1024];

        let mut offset_last_record = 0;
        let mut offset = 0;
        let mut start_offset = 0;
        let mut key_offset = 0;
        let mut value_offset = 0;

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
            offset_last_record = 0;
            total_read += bytes_read as u64;

            loop {
                if offset + 3 > bytes_read {
                    let slice_to_copy = &mut Vec::with_capacity(0);
                    chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                    offset_last_record = bytes_read - start_offset;
                    chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                    continue 'read;
                }
                start_offset = offset;
                let kl = (chunk[offset + 2] as u32) << 16 | (chunk[offset + 1] as u32) << 8 | (chunk[offset + 0] as u32);
                offset += 3;

                if offset + kl as usize + 3 /*3 here is bytes for kl*/ > bytes_read {
                    let slice_to_copy = &mut Vec::with_capacity(0);
                    chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                    offset_last_record = bytes_read - start_offset;
                    chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                    continue 'read;
                }
                key_offset = offset;
                offset += kl as usize;

                let vl = (chunk[offset + 2] as u32) << 16 | (chunk[offset + 1] as u32) << 8 | (chunk[offset + 0] as u32);
                offset += 3;

                if offset + vl as usize > bytes_read {
                    let slice_to_copy = &mut Vec::with_capacity(0);
                    chunk[start_offset..bytes_read].clone_into(slice_to_copy);
                    offset_last_record = bytes_read - start_offset;
                    chunk[0..offset_last_record].copy_from_slice(slice_to_copy);
                    continue 'read;
                }

                value_offset = offset;
                offset += vl as usize;

                self.insert(chunk[key_offset..key_offset+kl as usize].to_vec(), chunk[value_offset..value_offset+vl as usize].to_vec());
            }
        }
    }

    // NOT EXISTS!

    fn get_and_reset_cache_time(&self, _key: &Vec<u8>) -> Option<Vec<u8>> {
        unreachable!()
    }

    fn invalid_cache(&self) {
        unreachable!()
    }
}