use std::ops::{DerefMut};
use std::sync::Arc;
use crate::constants::actions;
use crate::server::server::write_msg;
use crate::server::stream_trait::Stream;
use crate::space::cache_space::CacheSpace;
use crate::space::in_memory_space::InMemorySpace;
use crate::space::space;
use crate::storage::storage::Storage;
use crate::utils::fastbytes::uint;

#[inline(always)]
pub fn create_space_in_memory(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_buf_offset: &mut usize) -> usize {
    if message.len() < 5 {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let mut spaces;
    let size = uint::u16(&message[1..3]);
    let is_it_logging = message[3] != 0;
    let name = String::from_utf8(message[4..].to_vec()).unwrap();
    match storage.spaces_names.write() {
        Ok(mut spaces_names) => {
            let mut i = 0;
            for exists_name in spaces_names.iter() {
                if *exists_name == name {
                    return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                }
                i += 1;
            }
            let mut offset = *log_buf_offset;
            log_buf[offset] = actions::CREATE_SPACE_IN_MEMORY;
            offset += 1;
            log_buf[offset] = message[3];
            offset += 1;
            let name_len = name.len();
            log_buf[offset] = name_len as u8;
            offset += 1;
            log_buf[offset] = (name_len >> 8) as u8;
            log_buf[offset..offset+name_len].copy_from_slice(name.as_bytes());
            offset += name.len();
            *log_buf_offset = offset;

            let mut file = storage.main_file.lock().unwrap();
            let mut buf = [0u8; 65535];
            buf[0] = space::IN_MEMORY;
            buf[1] = name_len as u8;
            buf[2] = (name_len >> 8) as u8;
            buf[3..3 + name_len].copy_from_slice(name.as_bytes());
            Stream::write_all(file.deref_mut(), &buf[..2 + name_len]).expect("Can't write to main storage file");

            spaces_names.push(name.clone());
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let spaces_not_unwrapped = storage.spaces.write();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let l = spaces.len();
    spaces.push(
        Box::new(InMemorySpace::new(l, size as usize, name, is_it_logging, 0))
    );
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_space_cache(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_buf_offset: &mut usize) -> usize {
    if message.len() < 9 {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let mut spaces;
    let size = uint::u16(&message[1..3]);
    let is_it_logging = message[3] != 0;
    let cache_duration = uint::u64(&message[4..12]);
    let name = String::from_utf8(message[12..].to_vec()).unwrap();
    match storage.spaces_names.write() {
        Ok(mut spaces_names) => {
            let mut i = 0;
            for exists_name in spaces_names.iter() {
                if *exists_name == name {
                    return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                }
                i += 1;
            }

            let mut offset = *log_buf_offset;
            log_buf[offset] = actions::CREATE_SPACE_IN_MEMORY;
            offset += 1;
            log_buf[offset] = message[3];
            offset += 1;
            let name_len = name.len();
            log_buf[offset] = name_len as u8;
            offset += 1;
            log_buf[offset] = (name_len >> 8) as u8;
            log_buf[offset..offset+name_len].copy_from_slice(name.as_bytes());
            offset += name.len();
            *log_buf_offset = offset;

            let mut file = storage.main_file.lock().unwrap();
            let mut buf = [0u8; 65535];
            buf[0] = space::IN_MEMORY;
            buf[1] = name_len as u8;
            buf[2] = (name_len >> 8) as u8;
            buf[3..3 + name_len].copy_from_slice(name.as_bytes());
            Stream::write_all(file.deref_mut(), &buf[..2 + name_len]).expect("Can't write to main storage file");

            spaces_names.push(name.clone());
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let spaces_not_unwrapped = storage.spaces.write();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }

    let l = spaces.len();
    spaces.push(
        Box::new(CacheSpace::new(l, size as usize, cache_duration, name, is_it_logging, 0))
    );
    storage.cache_spaces_indexes.write().unwrap().push(l);
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn get_spaces_names(stream: &mut impl Stream, storage: Arc<Storage>, write_buf: &mut [u8], write_offset: usize) -> usize {
    let spaces_names;
    let spaces_names_not_unwrapped = storage.spaces_names.read();
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