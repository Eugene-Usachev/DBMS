#[allow(unused)]

// TODO: maybe remove?

use std::fs::File;
use std::intrinsics::copy_nonoverlapping;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use crate::bin_types::{BinKey, BinValue};
use crate::writers::{get_size_for_key_len, get_size_for_value_len};

pub struct LogFile {
    pub file: Arc<Mutex<File>>,
}

impl LogFile {
    pub fn new(file: File) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
        }
    }
}

impl Drop for LogFile {
    fn drop(&mut self) {
        self.file.lock().unwrap().flush().unwrap();
    }
}

impl Write for LogFile {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.file.lock().unwrap().write_all(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.lock().unwrap().flush()
    }

    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.file.lock().unwrap().write_all(data)
    }
}

unsafe impl Send for LogFile {}
unsafe impl Sync for LogFile {}

impl Clone for LogFile {
    fn clone(&self) -> Self {
        Self {
            file: Arc::clone(&self.file),
        }
    }
}

pub struct LogWriter {
    writer: BufWriter<LogFile>,
}

const SIZE: usize = 65356;

impl LogWriter {
    pub fn new(log_file: LogFile) -> Self {
        Self {
            writer: BufWriter::with_capacity(SIZE, log_file),
        }
    }
    
    #[inline(always)]
    pub fn flush(&mut self) {
        if self.writer.buffer().len() > 0 {
            self.writer.flush().expect("Can't flush log writer!");
        }
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_action(&mut self, action: u8) {
        self.writer.write_all(&[action]).unwrap();
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_slice(&mut self, slice: &[u8]) {
        self.writer.write_all(slice).unwrap();
    }
    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_action_and_slice(&mut self, action: u8, slice: &[u8]) {
        let full_len = 1 + slice.len();
        if full_len + self.writer.buffer().len() > SIZE {
            if full_len < SIZE {
                self.writer.flush().expect("Can't flush log writer!");
            } else {
                let mut buf = Vec::with_capacity(full_len);
                buf.extend_from_slice(&[action]);
                buf.extend_from_slice(slice);
                self.writer.get_mut().write_all(&buf).expect("Can't write all buffer in log writer!");
                return;
            }
        }
        self.writer.write_all(&[action]).unwrap();
        self.writer.write_all(slice).unwrap();
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_key(&mut self, action: u8, table_number: u16, key: &BinKey) {
        let key_len = key.len();
        let key_size = get_size_for_key_len(key_len);
        let full_len = key_len + key_size + 3;
        if full_len + self.writer.buffer().len() > SIZE {
            if full_len < SIZE {
                self.writer.flush().expect("Can't flush log writer!");
            } else {
                let mut buf = Vec::with_capacity(full_len);
                buf.extend_from_slice(&[action, table_number as u8, (table_number >> 8) as u8]);
                buf.extend_from_slice(key.deref_all_with_len_and_size(key_len, key_size));
                self.writer.get_mut().write_all(&buf).expect("Can't write all buffer in log writer!");
                return;
            }
        }
        self.writer.write_all(&[action, table_number as u8, (table_number >> 8) as u8]).unwrap();
        self.writer.write_all(key.deref_all_with_len_and_size(key_len, key_size)).unwrap();
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_key_and_slice(&mut self, action: u8, table_number: u16, key: &BinKey, slice: &[u8]) {
        let key_len = key.len();
        let key_size = get_size_for_key_len(key_len);
        let full_len = key_len + key_size + 3 + slice.len();
        if full_len + self.writer.buffer().len() > SIZE {
            if full_len < SIZE {
                self.writer.flush().expect("Can't flush log writer!");
            } else {
                let mut buf = Vec::with_capacity(full_len);
                buf.extend_from_slice(&[action, table_number as u8, (table_number >> 8) as u8]);
                buf.extend_from_slice(key.deref_all_with_len_and_size(key_len, key_size));
                buf.extend_from_slice(slice);
                self.writer.get_mut().write_all(&buf).expect("Can't write all buffer in log writer!");
                return;
            }
        }
        self.writer.write_all(&[action, table_number as u8, (table_number >> 8) as u8]).unwrap();
        self.writer.write_all(key.deref_all_with_len_and_size(key_len, key_size)).unwrap();
        self.writer.write_all(slice).unwrap();
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_key_and_value(&mut self, action: u8, table_number: u16, key: &BinKey, value: &BinValue) {
        let key_len = key.len();
        let key_size = get_size_for_key_len(key_len);
        let value_len = value.len();
        let value_size = get_size_for_value_len(value_len);
        let full_len = key_len + key_size + 3 + value_size + value_len;
        if full_len + self.writer.buffer().len() > SIZE {
            if full_len < SIZE {
                self.writer.flush().expect("Can't flush log writer!");
            } else {
                let mut buf = Vec::with_capacity(full_len);
                buf.extend_from_slice(&[action, table_number as u8, (table_number >> 8) as u8]);
                buf.extend_from_slice(key.deref_all_with_len_and_size(key_len, key_size));
                buf.extend_from_slice(value.deref_all_with_len_and_size(value_len, value_size));
                self.writer.get_mut().write_all(&buf).expect("Can't write all buffer in log writer!");
                return;
            }
        }
        self.writer.write_all(&[action, table_number as u8, (table_number >> 8) as u8]).unwrap();
        self.writer.write_all(key.deref_all_with_len_and_size(key_len, key_size)).unwrap();
        self.writer.write_all(value.deref_all_with_len_and_size(value_len, value_size)).unwrap();
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub fn write_to_log_with_key_and_value_and_slice(&mut self, action: u8, table_number: u16, key: &BinKey, value: &BinValue, slice: &[u8]) {
        let key_len = key.len();
        let key_size = get_size_for_key_len(key_len);
        let value_len = value.len();
        let value_size = get_size_for_value_len(value_len);
        let full_len = key_len + key_size + 3 + value_size + value_len + slice.len();
        if full_len + self.writer.buffer().len() > SIZE {
            if full_len < SIZE {
                self.writer.flush().expect("Can't flush log writer!");
            } else {
                let mut buf = Vec::with_capacity(full_len);
                buf.extend_from_slice(&[action, table_number as u8, (table_number >> 8) as u8]);
                buf.extend_from_slice(key.deref_all_with_len_and_size(key_len, key_size));
                buf.extend_from_slice(value.deref_all_with_len_and_size(value_len, value_size));
                buf.extend_from_slice(slice);
                self.writer.get_mut().write_all(&buf).expect("Can't write all buffer in log writer!");
                return;
            }
        }
        self.writer.write_all(&[action, table_number as u8, (table_number >> 8) as u8]).unwrap();
        self.writer.write_all(key.deref_all_with_len_and_size(key_len, key_size)).unwrap();
        self.writer.write_all(value.deref_all_with_len_and_size(value_len, value_size)).unwrap();
        self.writer.write_all(slice).unwrap();
    }
}