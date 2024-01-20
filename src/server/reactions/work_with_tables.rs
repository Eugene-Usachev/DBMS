use std::intrinsics::unlikely;
use std::sync::Arc;
use crate::bin_types::{BinKey, BinValue};
use crate::constants::actions;
use crate::server::server::{write_msg, write_msg_with_status_separate};
use crate::server::stream_trait::Stream;
use crate::storage::storage::Storage;
use crate::utils::fastbytes::uint;

#[inline(always)]
pub fn get(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize) -> usize {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => unsafe {
            let res = table.get(&BinKey::new(&message[3..]));
            if unlikely(res.is_none()) {
                return write_msg(stream, write_buf, write_offset, &[actions::NOT_FOUND]);
            }
            let value = res.unwrap_unchecked();
            return write_msg_with_status_separate(stream, write_buf, write_offset, actions::DONE, value.deref())
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::TABLE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn get_field(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize) -> usize {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => unsafe {
            let field = uint::u16(&message[3..5]);
            let res = table.get_field(&BinKey::new(&message[5..]), field as usize);
            if unlikely(res.is_none()) {
                return write_msg(stream, write_buf, write_offset, &[actions::NOT_FOUND]);
            }
            let value = res.unwrap_unchecked();
            return write_msg_with_status_separate(stream, write_buf, write_offset, actions::DONE, &value)
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::TABLE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn get_fields(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize) -> usize {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => unsafe {
            let number_of_fields = uint::u16(&message[3..5]) as usize;
            let mut fields = Vec::with_capacity(number_of_fields);
            for i in 0..number_of_fields {
                fields.push(uint::u16(&message[5+i*2..5+i*2+2]) as usize);
            }
            let res = table.get_fields(&BinKey::new(&message[5..]), &fields);
            if unlikely(res.is_none()) {
                return write_msg(stream, write_buf, write_offset, &[actions::NOT_FOUND]);
            }
            let value = res.unwrap_unchecked();
            return write_msg_with_status_separate(stream, write_buf, write_offset, actions::DONE, &value)
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::TABLE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn insert(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = &message[5..5+key_size];
    let value = &message[5+key_size..];
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.insert(BinKey::new(key), BinValue::new(value), log_buf, log_offset);
            write_msg(stream, write_buf, write_offset, &[actions::DONE])
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::TABLE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn set(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = &message[5..5+key_size];
    let value = &message[5+key_size..];
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.set(BinKey::new(key), BinValue::new(value), log_buf, log_offset);
            write_msg(stream, write_buf, write_offset, &[actions::DONE])
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::TABLE_NOT_FOUND])
        }
    }
}

#[inline(always)]
pub fn delete(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    let key = &message[3..];
    match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.delete(&BinKey::new(key), log_buf, log_offset);
            write_msg(stream, write_buf, write_offset, &[actions::DONE])
        }
        None => {
            write_msg(stream, write_buf, write_offset, &[actions::TABLE_NOT_FOUND])
        }
    }
}