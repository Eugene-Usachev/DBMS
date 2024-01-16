use std::intrinsics::unlikely;
use std::sync::Arc;
use crate::constants::actions;
use crate::index::HashInMemoryIndex;
use crate::scheme::scheme::scheme_from_bytes;
use crate::server::server::write_msg;
use crate::server::stream_trait::Stream;
use crate::storage::storage::Storage;
use crate::utils::fastbytes::uint;
use crate::writers::{write_to_log_with_slice};

#[inline(always)]
pub fn create_table_in_memory(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    if unlikely(message.len() < 7) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let is_it_logging = message[1] != 0;
    let scheme_len = ((message[3] as u16) << 8 | message[2] as u16) as usize;
    if unlikely(scheme_len + 4 + 2 > message.len()) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let user_scheme = &message[4..4 + scheme_len as usize];
    let scheme = scheme_from_bytes(user_scheme);
    if unlikely(scheme.is_err()) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let name = String::from_utf8(message[4 + scheme_len..].to_vec()).unwrap();
    let name_len = name.len();
    {
        let mut buf = vec![0; name_len + 4];
        buf[0] = actions::CREATE_TABLE_IN_MEMORY;
        buf[1] = name_len as u8;
        buf[2] = (name_len >> 8) as u8;
        buf[3] = if is_it_logging { 1 } else { 0 };
        let mut offset = 4;
        buf[offset..offset + name_len].copy_from_slice(name.as_bytes());
        offset += name_len;
        buf[offset] = scheme_len as u8;
        offset += 1;
        buf[offset] = (scheme_len >> 8) as u8;
        offset += 1;
        buf[offset..].copy_from_slice(user_scheme);
        write_to_log_with_slice(log_buf, log_offset, &buf);
    }


    let l = Storage::create_in_memory_table(storage.clone(), name, HashInMemoryIndex::new(), is_it_logging, scheme.unwrap(), user_scheme);
    if unlikely(l == (u16::MAX - 1u16) as usize) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_table_on_disk(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    if unlikely(message.len() < 5) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let scheme_len = (message[2] as u16) << 8 | message[1] as u16;
    if unlikely(scheme_len as usize + 3 + 2 > message.len()) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let user_scheme = &message[4..4 + scheme_len as usize];
    let scheme = scheme_from_bytes(user_scheme);
    if unlikely(scheme.is_err()) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let name = String::from_utf8(message[1..].to_vec()).unwrap();
    let name_len = name.len();
    {
        let mut buf = vec![0; name_len + 3];
        buf[0] = actions::CREATE_TABLE_ON_DISK;
        buf[1] = name_len as u8;
        buf[2] = (name_len >> 8) as u8;
        let mut offset = 3;
        buf[offset..offset + name_len].copy_from_slice(name.as_bytes());
        offset += name_len;
        buf[offset] = scheme_len as u8;
        offset += 1;
        buf[offset] = (scheme_len >> 8) as u8;
        offset += 1;
        buf[offset..].copy_from_slice(user_scheme);
        write_to_log_with_slice(log_buf, log_offset, &buf);
    }

    let l = Storage::create_on_disk_table(storage.clone(), name, HashInMemoryIndex::new(), scheme.unwrap(), user_scheme);
    if unlikely(l == (u16::MAX - 1u16) as usize) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_table_cache(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    if unlikely(message.len() < 11) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let is_it_logging = message[1] != 0;
    let cache_duration = uint::u64(&message[2..10]);
    let scheme_len = ((message[11] as u16) << 8 | message[10] as u16) as usize;
    if unlikely(scheme_len + 12 + 2 > message.len()) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let user_scheme = &message[12..12 + scheme_len];
    let scheme = scheme_from_bytes(user_scheme);
    if unlikely(scheme.is_err()) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    let name = String::from_utf8(message[12 + scheme_len..].to_vec()).unwrap();
    let name_len = name.len();
    {
        let mut buf = vec![0; name_len + 12];
        buf[0] = actions::CREATE_TABLE_CACHE;
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
        let mut offset = 12;
        buf[offset..offset + name_len].copy_from_slice(name.as_bytes());
        offset += name_len;
        buf[offset] = scheme_len as u8;
        offset += 1;
        buf[offset] = (scheme_len >> 8) as u8;
        offset += 1;
        buf[offset..].copy_from_slice(user_scheme);
        write_to_log_with_slice(log_buf, log_offset, &buf);
    }

    let l = Storage::create_cache_table(storage.clone(), name, HashInMemoryIndex::new(), cache_duration, is_it_logging, scheme.unwrap(), user_scheme);
    if unlikely(l == (u16::MAX - 1u16) as usize) {
        return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
    }
    write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn get_tables_names(stream: &mut impl Stream, storage: Arc<Storage>, write_buf: &mut [u8], write_offset: usize) -> usize {
    let tables_names;
    let tables_names_not_unwrapped = storage.tables_names.read();
    match tables_names_not_unwrapped {
        Ok(tables_names_unwrapped) => {
            tables_names = tables_names_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }

    let mut local_buffer = [0u8;32367];
    let mut local_offset = 1;

    local_buffer[0] = actions::DONE;
    for name in tables_names.iter() {
        let l = name.len() as u16;
        local_buffer[local_offset..local_offset+2].copy_from_slice(&[l as u8, ((l >> 8) as u8)]);
        local_buffer[local_offset+2..local_offset+2+l as usize].copy_from_slice(name.as_bytes());
        local_offset += 2 + l as usize;
    }
    write_msg(stream, write_buf, write_offset, &local_buffer[..local_offset])
}