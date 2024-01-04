use std::intrinsics::unlikely;
use std::io::Write;
use std::sync::Arc;
use crate::constants::actions;
use crate::index::HashInMemoryIndex;
use crate::server::server::write_msg;
use crate::server::stream_trait::Stream;
use crate::storage::storage::Storage;
use crate::table::on_disk::OnDiskTable;
use crate::table::in_memory::InMemoryTable;
use crate::table::cache::CacheTable;
use crate::utils::fastbytes::uint;
use crate::writers::{LogWriter, write_to_log_with_slice};

#[inline(always)]
pub fn create_table_in_memory(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    if unlikely(message.len() < 5) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let mut spaces;
    let spaces_not_unwrapped = storage.tables.write();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let l = spaces.len();
    if unlikely(l == (u16::MAX - 1u16) as usize) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let size = uint::u16(&message[1..3]);
    let is_it_logging = message[3] != 0;
    let name = String::from_utf8(message[4..].to_vec()).unwrap();
    match storage.tables_names.write() {
        Ok(mut spaces_names) => {
            let mut i = 0;
            for exists_name in spaces_names.iter() {
                if unlikely(*exists_name == name) {
                    return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                }
                i += 1;
            }
            let name_len = name.len();
            {
                let mut buf = vec![0; name_len + 3];
                buf[0] = actions::CREATE_SPACE_IN_MEMORY;
                buf[1] = name_len as u8;
                buf[2] = (name_len >> 8) as u8;
                buf[3] = if is_it_logging { 1 } else { 0 };
                buf[4..].copy_from_slice(name.as_bytes());
                write_to_log_with_slice(log_buf, log_offset, &buf);
            }

            spaces_names.push(name.clone());
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    
    spaces.push(
        Box::new(InMemoryTable::new(l as u16, HashInMemoryIndex::new(), name, is_it_logging, 0))
    );
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_table_on_disk(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    if unlikely(message.len() < 5) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let mut spaces;
    let spaces_not_unwrapped = storage.tables.write();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let l = spaces.len();
    if unlikely(l == (u16::MAX - 1u16) as usize) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let size = uint::u16(&message[1..3]);
    let name = String::from_utf8(message[3..].to_vec()).unwrap();
    match storage.tables_names.write() {
        Ok(mut spaces_names) => {
            let mut i = 0;
            for exists_name in spaces_names.iter() {
                if unlikely(*exists_name == name) {
                    return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                }
                i += 1;
            }
            let name_len = name.len();
            {
                let mut buf = vec![0; name_len + 3];
                buf[0] = actions::CREATE_SPACE_ON_DISK;
                buf[1] = name_len as u8;
                buf[2] = (name_len >> 8) as u8;
                buf[3..].copy_from_slice(name.as_bytes());
                write_to_log_with_slice(log_buf, log_offset, &buf);
            }

            spaces_names.push(name.clone());
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    spaces.push(
        Box::new(OnDiskTable::new(name, 512, HashInMemoryIndex::new()))
    );
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_table_cache(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    if unlikely(message.len() < 9) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let mut spaces;
    let spaces_not_unwrapped = storage.tables.write();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let size = uint::u16(&message[1..3]);
    let is_it_logging = message[3] != 0;
    let cache_duration = uint::u64(&message[4..12]);
    let name = String::from_utf8(message[12..].to_vec()).unwrap();
    let l = spaces.len();
    if unlikely(l == (u16::MAX - 1u16) as usize) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    match storage.tables_names.write() {
        Ok(mut spaces_names) => {
            let mut i = 0;
            for exists_name in spaces_names.iter() {
                if unlikely(*exists_name == name) {
                    return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                }
                i += 1;
            }

            let name_len = name.len();
            {
                let mut buf = vec![0; name_len + 3];
                buf[0] = actions::CREATE_SPACE_CACHE;
                buf[1] = name_len as u8;
                buf[2] = (name_len >> 8) as u8;
                buf[3] = if is_it_logging { 1 } else { 0 };
                buf[4] = (cache_duration >> 56) as u8;
                buf[5] = (cache_duration >> 48) as u8;
                buf[6] = (cache_duration >> 40) as u8;
                buf[7] = (cache_duration >> 32) as u8;
                buf[8] = (cache_duration >> 24) as u8;
                buf[9] = (cache_duration >> 16) as u8;
                buf[10] = (cache_duration >> 8) as u8;
                buf[11] = cache_duration as u8;
                buf[12..].copy_from_slice(name.as_bytes());
                write_to_log_with_slice(log_buf, log_offset, &buf);
            }

            spaces_names.push(name.clone());
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    
   
    spaces.push(
        Box::new(CacheTable::new(l as u16, HashInMemoryIndex::new(), cache_duration, name, is_it_logging, 0))
    );
    storage.cache_tables_indexes.write().unwrap().push(l);
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn get_spaces_names(stream: &mut impl Stream, storage: Arc<Storage>, write_buf: &mut [u8], write_offset: usize) -> usize {
    let spaces_names;
    let spaces_names_not_unwrapped = storage.tables_names.read();
    match spaces_names_not_unwrapped {
        Ok(spaces_names_unwrapped) => {
            spaces_names = spaces_names_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }

    let mut local_buffer = [0u8;32367];
    let mut local_offset = 1;

    local_buffer[0] = actions::DONE;
    for name in spaces_names.iter() {
        let l = name.len() as u16;
        local_buffer[local_offset..local_offset+2].copy_from_slice(&[l as u8, ((l >> 8) as u8)]);
        local_buffer[local_offset+2..local_offset+2+l as usize].copy_from_slice(name.as_bytes());
        local_offset += 2 + l as usize;
    }
    write_msg(stream, write_buf, write_offset, &local_buffer[..local_offset])
}