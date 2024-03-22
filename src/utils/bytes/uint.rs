#[inline(always)]
#[allow(dead_code)]
pub fn u8(slice: &[u8]) -> u8 {
    slice[0]
}

#[inline(always)]
#[allow(dead_code)]
pub fn u16(slice: &[u8]) -> u16 {
    (slice[1] as u16) << 8 | slice[0] as u16
}

#[inline(always)]
#[allow(dead_code)]
pub fn u32(slice: &[u8]) -> u32 {
    (slice[3] as u32) << 24 | (slice[2] as u32) << 16 | (slice[1] as u32) << 8 | slice[0] as u32
}

#[inline(always)]
#[allow(dead_code)]
pub fn u64(slice: &[u8]) -> u64 {
    (slice[7] as u64) << 56 | (slice[6] as u64) << 48 | (slice[5] as u64) << 40 | (slice[4] as u64) << 32 | (slice[3] as u64) << 24 | (slice[2] as u64) << 16 | (slice[1] as u64) << 8 | slice[0] as u64
}

#[inline(always)]
#[allow(dead_code)]
pub fn u8tob(u: u8) -> [u8; 1] {
    [u]
}

#[inline(always)]
#[allow(dead_code)]
pub fn u16tob(u: u16) -> [u8; 2] {
    [u as u8, (u >> 8) as u8]
}

#[inline(always)]
#[allow(dead_code)]
pub fn u32tob(u: u32) -> [u8; 4] {
    [u as u8, (u >> 8) as u8, (u >> 16) as u8, (u >> 24) as u8]
}

#[inline(always)]
#[allow(dead_code)]
pub fn u64tob(u: u64) -> [u8; 8] {
    [u as u8, (u >> 8) as u8, (u >> 16) as u8, (u >> 24) as u8, (u >> 32) as u8, (u >> 40) as u8, (u >> 48) as u8, (u >> 56) as u8]
}