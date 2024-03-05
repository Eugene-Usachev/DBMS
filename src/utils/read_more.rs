#[inline(always)]
pub fn read_more(chunk: &mut [u8], start_offset: usize, bytes_read: usize, offset_last_record: &mut usize) {
    let slice_to_copy = &mut Vec::with_capacity(0);
    chunk[start_offset..bytes_read].clone_into(slice_to_copy);
    *offset_last_record = bytes_read - start_offset;
    chunk[0..*offset_last_record].copy_from_slice(slice_to_copy);
}