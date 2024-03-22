#[inline(always)]
#[allow(dead_code)]
pub fn i8(slice: &[u8]) -> i8 {
    slice[0] as i8
}

#[inline(always)]
#[allow(dead_code)]
pub fn i16(slice: &[u8]) -> i16 {
    (slice[1] as i16) << 8 | slice[0] as i16
}

#[inline(always)]
#[allow(dead_code)]
pub fn i32(slice: &[u8]) -> i32 {
    (slice[3] as i32) << 24 | (slice[2] as i32) << 16 | (slice[1] as i32) << 8 | slice[0] as i32
}

#[inline(always)]
#[allow(dead_code)]
pub fn i64(slice: &[u8]) -> i64 {
    (slice[7] as i64) << 56 | (slice[6] as i64) << 48 | (slice[5] as i64) << 40 | (slice[4] as i64) << 32 | (slice[3] as i64) << 24 | (slice[2] as i64) << 16 | (slice[1] as i64) << 8 | slice[0] as i64
}

#[inline(always)]
#[allow(dead_code)]
pub fn itob(i: i8) -> [u8; 1] {
    [i as u8]
}

#[inline(always)]
#[allow(dead_code)]
pub fn i16tob(i: i16) -> [u8; 2] {
    [i as u8, (i >> 8) as u8]
}

#[inline(always)]
#[allow(dead_code)]
pub fn i32tob(i: i32) -> [u8; 4] {
    [i as u8, (i >> 8) as u8, (i >> 16) as u8, (i >> 24) as u8]
}

#[inline(always)]
#[allow(dead_code)]
pub fn i64tob(i: i64) -> [u8; 8] {
    [i as u8, (i >> 8) as u8, (i >> 16) as u8, (i >> 24) as u8, (i >> 32) as u8, (i >> 40) as u8, (i >> 48) as u8, (i >> 56) as u8]
}