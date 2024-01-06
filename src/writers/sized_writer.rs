use std::io::{BufWriter, Read, Write};
use crate::bin_types::{BinKey, BinValue};

pub struct SizedWriter<T: Write + Read> {
    pub inner: BufWriter<T>
}

impl<T: Write + Read> SizedWriter<T> {
    pub fn new_with_capacity(writer: T, capacity: usize) -> SizedWriter<T> {
        SizedWriter {
            inner: BufWriter::with_capacity(capacity, writer)
        }
    }
    #[inline(always)]
    /// It calls write_all
    pub fn write(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    #[inline(always)]
    pub fn write_key(&mut self, key: &BinKey) -> std::io::Result<()> {
        let l = key.len();
        return self.inner.write_all(&key.deref_all_with_len(l));
    }

    #[inline(always)]
    pub fn write_key_with_size(&mut self, key: &BinKey, size: usize) -> std::io::Result<()> {
        let l = key.len();
        return self.inner.write_all(&key.deref_all_with_len_and_size(l, size));
    }

    #[inline(always)]
    pub fn write_value(&mut self, key: &BinValue) -> std::io::Result<()> {
        let l = key.len();
        return self.inner.write_all(&key.deref_all_with_len(l));
    }

    #[inline(always)]
    pub fn write_value_with_size(&mut self, key: &BinValue, size: usize) -> std::io::Result<()> {
        let l = key.len();
        return self.inner.write_all(&key.deref_all_with_len_and_size(l, size));
    }
}