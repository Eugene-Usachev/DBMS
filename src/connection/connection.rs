use std::fmt::{Display};
use std::io::{BufWriter, Read, Write};
use crate::stream::Stream;
use crate::utils::fastbytes::uint;
use crate::utils::fastbytes::uint::u16;

const BUFFER_SIZE: usize = u16::MAX as usize;

pub struct BufReader<R: Read> {
    pub buf: [u8; BUFFER_SIZE],
    pub big_buf: Vec<u8>,
    pub reader: R,
    pub read_offset: usize,
    pub write_offset: usize,
    pub request_size: usize,
}

impl<R: Read> BufReader<R> {
    pub fn new(reader: R) -> Self {
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

#[repr(u8)]
#[derive(PartialEq)]
pub enum Status {
    Ok,
    All,
    Closed,
    Error
}

pub struct BufConnection<S: Stream> {
    reader: BufReader<S>,
    writer: BufWriter<S>,
}

impl<'a, S: Stream> BufConnection<S> {
    pub fn new(mut stream: S) -> Self {
        let clone = stream.clone_ptr();
        let reader = BufReader::new(clone);
        let writer = BufWriter::with_capacity(BUFFER_SIZE, stream);

        Self {
            reader,
            writer,
        }
    }

    #[inline(always)]
    pub fn reader(&'a mut self) -> &'a mut BufReader<S> {
        &mut self.reader
    }

    #[inline(always)]
    pub fn stream(&'a mut self) -> &'a mut S {
        self.writer.get_mut()
    }

    #[inline(always)]
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    #[inline(always)]
    fn read_more(reader: &mut BufReader<S>, mut before: usize) -> Status {
        if before > BUFFER_SIZE {
            let left = reader.write_offset - reader.read_offset;
            let buf = Vec::from(&reader.buf[reader.read_offset..reader.write_offset]);
            reader.buf[0..left].copy_from_slice(&buf);
            reader.read_offset = 0;
            // need to read
            before = before - reader.write_offset;
            reader.write_offset = left;

            if before > BUFFER_SIZE {
                reader.big_buf.resize(before, 0);
                let mut read = 0;
                loop {
                    match Stream::read(&mut reader.reader, &mut reader.big_buf[read..before]) {
                        Ok(0) => {
                            return Status::Closed;
                        }
                        Ok(size) => {
                            read += size;
                            if read >= before {
                                return Status::Ok;
                            }
                        }
                        Err(e) => {
                            println!("Read connection error: {:?}", e);
                            return Status::Error;
                        }
                    };
                }
            }
        }
        loop {
            match Stream::read(&mut reader.reader, &mut reader.buf[reader.write_offset..]) {
                Ok(0) => {
                    return Status::Closed;
                }
                Ok(size) => {
                    reader.write_offset += size;
                    if reader.write_offset >= before {
                        return Status::Ok;
                    }
                }
                Err(e) => {
                    println!("Read connection error: {:?}", e);
                    return Status::Error;
                }
            };
        }
    }

    #[inline(always)]
    pub fn read_request(&mut self) -> Status {
        let mut reader = &mut self.reader;
        reader.write_offset = 0;
        let status = Self::read_more(&mut reader, 4);
        if status != Status::Ok {
            return status;
        }
        reader.request_size = uint::u32(&reader.buf[0..4]) as usize - 4;
        reader.read_offset = 4;
        return Status::Ok;
    }

    #[inline(always)]
    pub fn read_message(&mut self) -> (&'a [u8], Status) {
        if self.reader.big_buf.capacity() > 0 {
            self.reader.big_buf = Vec::with_capacity(0);
        }
        if self.reader.request_size == 0 {
            return (&[], Status::All);
        }
        let mut reader = &mut self.reader;
        let mut read_for = reader.read_offset + 2;
        if reader.write_offset < reader.read_offset + 2 {
            let status = Self::read_more(&mut reader, read_for);
            if status != Status::Ok {
                return (&[], status);
            }
        }

        let mut size = u16(&reader.buf[reader.read_offset..reader.read_offset + 2]) as usize;
        reader.read_offset += 2;
        reader.request_size -= 2;
        if size == u16::MAX as usize {
            read_for = reader.read_offset + 4;
            if reader.write_offset < read_for {
                let status = Self::read_more(&mut reader, read_for);
                if status != Status::Ok {
                    return (&[], status);
                }
            }
            size = uint::u32(&reader.buf[reader.read_offset..reader.read_offset + 4]) as usize;
            reader.read_offset += 4;
            reader.request_size -= 4;
        }

        read_for = reader.read_offset + size;
        if reader.write_offset < read_for {
            let status = Self::read_more(&mut reader, read_for);
            if status != Status::Ok {
                return (&[], status);
            }
        }

        reader.request_size -= size;
        if size < u16::MAX as usize {
            reader.read_offset += size;
            let ptr = &reader.buf[reader.read_offset - size..reader.read_offset];
            return (unsafe {std::mem::transmute::<&[u8], &'a [u8]>(ptr)}, Status::Ok);
        }

        let prt = &reader.big_buf[..size];
        return (unsafe {std::mem::transmute::<&[u8], &'a [u8]>(prt)}, Status::Ok);
    }

    #[inline(always)]
    pub fn write_message(&mut self, message: &[u8]) -> Status {
        let message_len = message.len();
        let mut res;
        if message_len < u16::MAX as usize {
            res = self.writer.write_all(&[message_len as u8, (message_len >> 8) as u8]);
        } else {
            res = self.writer.write_all(&[255, 255, message_len as u8, (message_len >> 8) as u8, (message_len >> 16) as u8, (message_len >> 24) as u8]);
        }

        if res.is_err() {
            return Status::Error;
        }

        res = self.writer.write_all(message);
        if res.is_err() {
            return Status::Error;
        }
        return Status::Ok;
    }

    #[inline(always)]
    pub fn write_message_and_status(&mut self, message: &[u8], status: u8) -> Status {
        let message_len = message.len() + 1;
        let mut res;
        if message_len < u16::MAX as usize {
            res = self.writer.write_all(&[message_len as u8, (message_len >> 8) as u8]);
        } else {
            res = self.writer.write_all(&[255, 255, message_len as u8, (message_len >> 8) as u8, (message_len >> 16) as u8, (message_len >> 24) as u8]);
        }

        if res.is_err() {
            return Status::Error;
        }

        res = self.writer.write_all(&[status]);
        if res.is_err() {
            return Status::Error;
        }

        res = self.writer.write_all(message);
        if res.is_err() {
            return Status::Error;
        }
        return Status::Ok;
    }

    #[inline(always)]
    pub fn close(&mut self) -> std::io::Result<()> {
        self.writer.get_mut().shutdown()
    }
}