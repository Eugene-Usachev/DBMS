use std::hash::{Hash, Hasher};
use std::intrinsics::{likely, unlikely};
use std::ptr;

pub struct BinValue {
    pub ptr: *mut u8
}

impl<'a> BinValue {
    pub fn new(slice: &[u8]) -> Self {
        let len = slice.len();
        let new_slice: *mut u8;
        let size;
        unsafe {
            if likely(len < 65535) {
                new_slice = Vec::<u8>::with_capacity(len + 2).leak().as_mut_ptr();
                *new_slice.offset(0) = len as u8;
                *new_slice.offset(1) = (len >> 8) as u8;
                size = 2;
            } else {
                new_slice = Vec::<u8>::with_capacity(len + 6).leak().as_mut_ptr();
                *new_slice.offset(0) = 255u8;
                *new_slice.offset(1) = 255u8;
                *new_slice.offset(2) = len as u8;
                *new_slice.offset(3) = (len >> 8) as u8;
                *new_slice.offset(4) = (len >> 16) as u8;
                *new_slice.offset(5) = (len >> 24) as u8;
                size = 6;
            }
            ptr::copy_nonoverlapping(slice.as_ptr(), new_slice.offset(size), len);
        }
        BinValue {
            ptr: new_slice
        }
    }

    #[inline(always)]
    pub fn deref(&self) -> &'a [u8] {
        self.deref_with_len(self.len())
    }

    #[inline(always)]
    pub fn deref_with_len(&self, len: usize) -> &'a [u8] {
        unsafe {
            if unlikely(len > 65354) {
                return &(*ptr::slice_from_raw_parts(self.ptr.add(6), len));
            } else {
                return &(*ptr::slice_from_raw_parts(self.ptr.add(2), len));
            }
        }
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn deref_all(&self) -> &'a [u8] {
        let len = self.len();
        self.deref_all_with_len(len)
    }

    #[inline(always)]
    pub fn deref_all_with_len_and_size(&self, len: usize, size: usize) -> &'a [u8] {
        unsafe {
            return &(*ptr::slice_from_raw_parts(self.ptr, len + size));
        }
    }

    #[inline(always)]
    pub fn deref_all_with_len(&self, len: usize) -> &'a [u8] {
        unsafe {
            if unlikely(len > 65354) {
                return &(*ptr::slice_from_raw_parts(self.ptr, len + 6));
            } else {
                return &(*ptr::slice_from_raw_parts(self.ptr, len + 2));
            }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let mut l;
        unsafe {
            // first byte is length
            l = (*self.ptr) as usize | (*self.ptr.offset(1) as usize) << 8;
            if unlikely(l == 65535) {
                l = (*self.ptr.offset(2) as usize) | (*self.ptr.offset(3) as usize) << 8 | (*self.ptr.offset(4) as usize) << 16 | (*self.ptr.offset(5) as usize) << 24;
            }
        }

        return l;
    }
}

impl Drop for BinValue {
    fn drop(&mut self) {
        let len = self.len();
        let size = if likely(len < 65535) { 2 } else { 6 };
        unsafe {
            Vec::from_raw_parts(self.ptr, len + size, len + size);
        }
    }
}

impl PartialEq<Self> for BinValue {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for BinValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Eq for BinValue {}

impl Hash for BinValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }

    fn hash_slice<H: Hasher>(data: &[BinValue], state: &mut H) {
        for i in data {
            i.hash(state);
        }
    }
}

impl Clone for BinValue {
    fn clone(&self) -> Self {
        let len = self.len();
        let size;
        let new_slice: *mut u8;
        if likely(len < 65535) {
            size = 2;
        } else {
            size = 6;
        }
        unsafe {
            new_slice = Vec::<u8>::with_capacity(len + size).leak().as_mut_ptr();
            ptr::copy_nonoverlapping(self.ptr, new_slice, len + size);
        }
        return BinValue {
            ptr: new_slice
        }
    }
}

impl std::fmt::Debug for BinValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl std::fmt::Display for BinValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl Ord for BinValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deref().cmp(other.deref())
    }
}

impl Default for BinValue {
    fn default() -> Self {
        BinValue {
            ptr: ptr::null_mut()
        }
    }
}

unsafe impl Send for BinValue {}

unsafe impl Sync for BinValue {}