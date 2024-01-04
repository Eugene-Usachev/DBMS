#[allow(unused)]

// TODO: maybe remove?

use std::fs::File;
use std::intrinsics::{likely, unlikely};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

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
    pub inner: LogFile,
    pub buf: [u8; u16::MAX as usize],
    pub bytes_written: usize,
}

impl LogWriter {
    pub fn new(file: LogFile) -> Self {
        Self {
            //inner: BufWriter::with_capacity(u16::MAX as usize, file),
            inner: file,
            buf: [0; u16::MAX as usize],
            bytes_written: 0,
        }
    }

    #[inline(always)]
    pub fn write(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut written = self.buf.len();
        let buf_len = buf.len();
        let mut new_written = written + buf_len;
        if unlikely(new_written > u16::MAX as usize) {
            if likely(buf_len < u16::MAX as usize) {
                self.flush()?;
                new_written = buf_len;
                written = 0;
            } else {
                return self.inner.write_all(buf);
            }
        }
        self.buf[written..new_written].copy_from_slice(buf);
        self.bytes_written += new_written;
        Ok(())
    }

    #[inline(always)]
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.inner.write(&self.buf[..self.bytes_written])?;
        self.bytes_written = 0;
        self.inner.flush()
    }
}