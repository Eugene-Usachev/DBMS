use std::io::Read;
use crate::connection::{
    status::Status,
    connection::BufReader as BufReaderTrait,
    BUFFER_SIZE
};
use crate::error;
use crate::stream::Stream;
use crate::utils::bytes::uint::{u16, u32};

pub struct BufReader<S: Stream> {
    pub buf: [u8; BUFFER_SIZE],
    pub big_buf: Vec<u8>,
    pub reader: S,
    pub read_offset: usize,
    pub write_offset: usize,
    pub request_size: usize,
}

impl<S: Stream> BufReader<S> {
    pub fn new(reader: S) -> Self {
        Self {
            buf: [0; BUFFER_SIZE],
            big_buf: Vec::with_capacity(0),
            reader,
            read_offset: 0,
            write_offset: 0,
            request_size: 0,
        }
    }
}

impl<'stream, S: Stream> BufReaderTrait<'stream, S> for BufReader<S> {
    #[inline(always)]
    fn read_more(&mut self, needed: usize) -> Status {
        if needed > BUFFER_SIZE {
            self.big_buf.resize(needed, 0);
            let mut read = self.write_offset - self.read_offset;
            self.big_buf[0..read].copy_from_slice(&self.buf[self.read_offset..self.write_offset]);
            self.write_offset = 0;
            self.read_offset = 0;
            loop {
                match Stream::read(&mut self.reader, &mut self.big_buf[read..needed]) {
                    Ok(0) => {
                        return Status::Closed;
                    }
                    Ok(size) => {
                        read += size;
                        if read >= needed {
                            return Status::Ok;
                        }
                    }
                    Err(e) => {
                        error!("Read connection error: {:?}", e);
                        return Status::Error;
                    }
                };
            }
        }
        if needed > BUFFER_SIZE - self.write_offset {
            let left = self.write_offset - self.read_offset;
            let buf = Vec::from(&self.buf[self.read_offset..self.write_offset]);
            self.buf[0..left].copy_from_slice(&buf);
            self.read_offset = 0;
            self.write_offset = left;
        }

        let mut read = 0;
        loop {
            match Stream::read(&mut self.reader, &mut self.buf[self.write_offset..]) {
                Ok(0) => {
                    return Status::Closed;
                }
                Ok(size) => {
                    self.write_offset += size;
                    read += size;
                    if read >= needed {
                        return Status::Ok;
                    }
                }
                Err(e) => {
                    error!("Read connection error: {:?}", e);
                    return Status::Error;
                }
            };
        }
    }

    /// read request returns status and is a request reading.
    #[inline(always)]
    fn read_request(&mut self) -> (Status, bool) {
        self.write_offset = 0;
        let status = self.read_more(5);
        if status != Status::Ok {
            return (status, false);
        }
        self.request_size = u32(&self.buf[0..4]) as usize;
        self.read_offset = 5;
        return (Status::Ok, self.buf[4] == 1);
    }

    #[inline(always)]
    fn read_message(&mut self) -> (&'stream [u8], Status) {
        if self.big_buf.capacity() > 0 {
            self.big_buf = Vec::with_capacity(0);
        }
        if self.request_size == 0 {
            return (&[], Status::All);
        }
        if self.write_offset < self.read_offset + 2 {
            let status = self.read_more(2);
            if status != Status::Ok {
                return (&[], status);
            }
        }

        let mut len = u16(&self.buf[self.read_offset..self.read_offset + 2]) as usize;
        self.read_offset += 2;
        self.request_size -= 2;
        if len == u16::MAX as usize {
            if self.write_offset < self.read_offset + 4 {
                let status = self.read_more(4);
                if status != Status::Ok {
                    return (&[], status);
                }
            }
            len = u32(&self.buf[self.read_offset..self.read_offset + 4]) as usize;
            self.read_offset += 4;
            self.request_size -= 4;
        }

        if self.write_offset < self.read_offset + len {
            let status = self.read_more(len);
            if status != Status::Ok {
                return (&[], status);
            }
        }

        self.request_size -= len;
        if len < u16::MAX as usize {
            self.read_offset += len;
            let ptr = &self.buf[self.read_offset - len..self.read_offset];
            return (unsafe {std::mem::transmute::<&[u8], &'stream [u8]>(ptr)}, Status::Ok);
        }

        let prt = &self.big_buf[..len];
        return (unsafe {std::mem::transmute::<&[u8], &'stream [u8]>(prt)}, Status::Ok);
    }
}