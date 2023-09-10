#[inline(always)]
pub(crate) fn get_hash(key: &[u8]) -> u64 {
    let mut res :u64 = 0;
    for i in 0..key.len() {
        res += (res << 5) - res + key[i] as u64;
    }
    res
}