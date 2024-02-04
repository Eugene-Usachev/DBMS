use std::hash::{Hash, Hasher};
use std::ptr;

pub struct BinKey {
    pub ptr: *mut u8
}

impl<'a> BinKey {
    pub fn new(slice: &[u8]) -> Self {
        let len = slice.len();
        let new_slice: *mut u8;
        let size;
        unsafe {
            if len < 255 {
                new_slice = Vec::<u8>::with_capacity(len + 1).leak().as_mut_ptr();
                *new_slice.offset(0) = len as u8;
                size = 1;
            } else {
                new_slice = Vec::<u8>::with_capacity(len + 3).leak().as_mut_ptr();
                *new_slice.offset(0) = 255u8;
                *new_slice.offset(1) = len as u8;
                *new_slice.offset(2) = (len >> 8) as u8;
                size = 3;
            }
            ptr::copy_nonoverlapping(slice.as_ptr(), new_slice.offset(size), len);
        }
        BinKey {
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
            if len < 255 {
                return &(*ptr::slice_from_raw_parts(self.ptr.add(1), len));
            }
            &(*ptr::slice_from_raw_parts(self.ptr.add(3), len))

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
            if len < 255 {
                return &(*ptr::slice_from_raw_parts(self.ptr, len + 1));
            }
            return &(*ptr::slice_from_raw_parts(self.ptr, len + 3));
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let l;
        unsafe {
            // first byte is length
            l = (*self.ptr) as usize;
            if l < 255 {
                return l;
            }
            // we take the second byte and third byte
            return (*self.ptr.offset(1) as usize) | (*self.ptr.offset(2) as usize) << 8;
        }
    }
}

impl Drop for BinKey {
    fn drop(&mut self) {
        let len = self.len();
        let size = if len < 255 { 1 } else { 3 };
        unsafe {
            Vec::from_raw_parts(self.ptr, len + size, len + size);
        }
    }
}

impl PartialEq<Self> for BinKey {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for BinKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Eq for BinKey {}

impl Hash for BinKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }

    fn hash_slice<H: Hasher>(data: &[BinKey], state: &mut H) {
        for i in data {
            i.hash(state);
        }
    }
}

impl Clone for BinKey {
    fn clone(&self) -> Self {
        let len = self.len();
        let size;
        let new_slice: *mut u8;
        unsafe {
            if len < 255 {
                new_slice = Vec::<u8>::with_capacity(len + 1).leak().as_mut_ptr();
                size = 1;
            } else {
                new_slice = Vec::<u8>::with_capacity(len + 3).leak().as_mut_ptr();
                size = 3;
            }
            ptr::copy_nonoverlapping(self.ptr, new_slice, len + size);
        }
        return BinKey {
            ptr: new_slice
        }
    }
}

impl std::fmt::Debug for BinKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl std::fmt::Display for BinKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl Ord for BinKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deref().cmp(other.deref())
    }
}

impl Default for BinKey {
    fn default() -> Self {
        BinKey {
            ptr: ptr::null_mut()
        }
    }
}

unsafe impl Send for BinKey {}

unsafe impl Sync for BinKey {}