use std::ptr::copy_nonoverlapping;
use crate::bin_types::{BinKey, BinValue};
use crate::writers::get_size_for_key_len;

#[inline(always)]
fn write_to_log(log_buf: &mut [u8], log_offset: &mut usize, action: u8) {
    log_buf[*log_offset] = action;
    *log_offset += 1;
}

#[inline(always)]
pub fn write_to_log_with_slice(log_buf: &mut [u8], log_offset: &mut usize, slice: &[u8]) {
    let len = slice.len();
    let offset = *log_offset;
    log_buf[offset..offset+len].copy_from_slice(slice);
}
#[inline(always)]
pub fn write_to_log_with_action_and_slice(log_buf: &mut [u8], log_offset: &mut usize, action: u8, slice: &[u8]) {
    let mut offset = *log_offset;
    log_buf[offset] = action;
    let len = slice.len();
    offset = offset + 1;
    log_buf[offset..offset+len].copy_from_slice(slice);
    *log_offset = offset + len;
}

#[inline(always)]
pub fn write_to_log_with_key(log_buf: &mut [u8], log_offset: &mut usize, action: u8, table_number: u16, key: &BinKey) {
    let mut offset = *log_offset;
    log_buf[offset] = action;
    offset = offset + 1;
    log_buf[offset] = table_number as u8;
    offset = offset + 1;
    log_buf[offset] = (table_number >> 8) as u8;
    offset = offset + 1;
    let key_len = key.len();
    let key_size = get_size_for_key_len(key_len);
    let key_all_size = key_len + key_size;
    unsafe { copy_nonoverlapping(key.ptr, log_buf[offset..offset+key_all_size].as_mut_ptr(), key_all_size); }
    *log_offset = offset + key_all_size;
}

#[inline(always)]
pub fn write_to_log_with_key_and_slice(log_buf: &mut [u8], log_offset: &mut usize, action: u8, table_number: u16, key: &BinKey, slice: &[u8]) {
    let mut offset = *log_offset;
    log_buf[offset] = action;
    offset = offset + 1;
    log_buf[offset] = table_number as u8;
    offset = offset + 1;
    log_buf[offset] = (table_number >> 8) as u8;
    offset = offset + 1;
    let key_len = key.len();
    let key_size = get_size_for_key_len(key_len);
    let key_all_size = key_len + key_size;
    unsafe { copy_nonoverlapping(key.ptr, log_buf[offset..offset+key_all_size].as_mut_ptr(), key_all_size); }
    offset = offset + key_all_size;
    let slice_len = slice.len();
    log_buf[offset..offset+slice_len].copy_from_slice(slice);
    *log_offset = offset + slice_len;
}

#[inline(always)]
pub fn write_to_log_with_key_and_value(log_buf: &mut [u8], log_offset: &mut usize, action: u8, table_number: u16, key: &BinKey, value: &BinValue) {
    let mut offset = *log_offset;
    log_buf[offset] = action;
    offset = offset + 1;
    log_buf[offset] = table_number as u8;
    offset = offset + 1;
    log_buf[offset] = (table_number >> 8) as u8;
    offset = offset + 1;
    let key_len = key.len();
    let key_size = get_size_for_key_len(key_len);
    let key_all_size = key_len + key_size;
    unsafe { copy_nonoverlapping(key.ptr, log_buf[offset..offset+key_all_size].as_mut_ptr(), key_all_size); }
    offset = offset + key_all_size;
    let value_len = value.len();
    let value_size = get_size_for_key_len(value_len);
    let value_all_size = value_len + value_size;
    unsafe { copy_nonoverlapping(value.ptr, log_buf[offset..offset+value_all_size].as_mut_ptr(), value_all_size); }
    *log_offset = offset + value_all_size;
}

#[inline(always)]
pub fn write_to_log_with_key_and_value_and_slice(log_buf: &mut [u8], log_offset: &mut usize, action: u8, table_number: u16, key: &BinKey, value: &BinValue, slice: &[u8]) {
    let mut offset = *log_offset;
    log_buf[offset] = action;
    offset = offset + 1;
    log_buf[offset] = table_number as u8;
    offset = offset + 1;
    log_buf[offset] = (table_number >> 8) as u8;
    offset = offset + 1;
    let key_len = key.len();
    let key_size = get_size_for_key_len(key_len);
    let key_all_size = key_len + key_size;
    unsafe { copy_nonoverlapping(key.ptr, log_buf[offset..offset+key_all_size].as_mut_ptr(), key_all_size); }
    offset = offset + key_all_size;
    let value_len = value.len();
    let value_size = get_size_for_key_len(value_len);
    let value_all_size = value_len + value_size;
    unsafe { copy_nonoverlapping(value.ptr, log_buf[offset..offset+value_all_size].as_mut_ptr(), value_all_size); }
    offset = offset + value_all_size;
    let slice_len = slice.len();
    log_buf[offset..offset+slice_len].copy_from_slice(slice);
    *log_offset = offset + slice_len;
}