use std::sync::Arc;
use crate::constants::actions;
use crate::server::server::{write_msg, write_msg_with_status_separate};
use crate::server::stream_trait::Stream;
use crate::storage::storage::Storage;
use crate::utils::fastbytes::uint;

#[inline(always)]
pub fn get(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize) -> usize {
    let spaces;
    let spaces_not_unwrapped = storage.spaces.read();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    match spaces.get(uint::u16(&message[1..3]) as usize) {
        Some(space) => unsafe {
            let res = space.get(&message[3..]);
            if res.is_none() {
                return write_msg(stream, write_buf, write_offset, &[actions::NOT_FOUND]);
            }
            let value = res.unwrap_unchecked();
            return write_msg_with_status_separate(stream, write_buf, write_offset, actions::DONE, value.as_slice())
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn get_and_reset_cache_time(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize) -> usize {
    let spaces;
    let spaces_not_unwrapped = storage.spaces.read();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    match spaces.get(uint::u16(&message[1..3]) as usize) {
        Some(space) => unsafe {
            let res = space.get_and_reset_cache_time(&message[3..]);
            if res.is_none() {
                return write_msg(stream, write_buf, write_offset, &[actions::NOT_FOUND]);
            }
            let value = res.unwrap_unchecked();
            return write_msg_with_status_separate(stream, write_buf, write_offset, actions::DONE, value.as_slice())
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn insert(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buffer: &mut [u8], log_buffer_offset: &mut usize) -> usize {
    let spaces;
    let spaces_not_unwrapped = storage.spaces.read();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = message[5..5+key_size].to_vec();
    let value = message[5+key_size..].to_vec();
    return match spaces.get(uint::u16(&message[1..3]) as usize) {
        Some(space) => {
            space.insert(key, value, log_buffer, log_buffer_offset);
            write_msg(stream, write_buf, write_offset, &[actions::DONE])
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn set(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buffer: &mut [u8], log_buffer_offset: &mut usize) -> usize {
    let spaces;
    let spaces_not_unwrapped = storage.spaces.read();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = message[5..5+key_size].to_vec();
    let value = message[5+key_size..].to_vec();
    return match spaces.get(uint::u16(&message[1..3]) as usize) {
        Some(space) => {
            space.set(key, value, log_buffer, log_buffer_offset);
            write_msg(stream, write_buf, write_offset, &[actions::DONE])
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn delete(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buffer: &mut [u8], log_buffer_offset: &mut usize) -> usize {
    let spaces;
    let spaces_not_unwrapped = storage.spaces.read();
    match spaces_not_unwrapped {
        Ok(spaces_unwrapped) => {
            spaces = spaces_unwrapped;
        }
        Err(_) => {
            return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
        }
    }
    let key = message[3..].to_vec();
    match spaces.get(uint::u16(&message[1..3]) as usize) {
        Some(space) => {
            space.delete(key, log_buffer, log_buffer_offset);
            write_msg(stream, write_buf, write_offset, &[actions::DONE])
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
        }
    }
}