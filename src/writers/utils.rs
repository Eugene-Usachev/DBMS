use std::intrinsics::likely;
use std::io::Write;
use std::sync::Arc;
use crate::bin_types::BinKey;
use crate::writers::PipeWriter;

#[inline(always)]
pub fn get_size_for_key_len(key_len: usize) -> usize {
    if likely(key_len < 255) {
        return 1;
    }
    3
}

#[inline(always)]
pub fn get_size_for_value_len(value_len: usize) -> usize {
    if likely(value_len < 65535) {
        return 2;
    }
    6
}