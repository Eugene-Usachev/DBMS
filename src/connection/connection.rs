use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use crate::utils::fastbytes::uint;
use crate::utils::fastbytes::uint::u16;

const BUFFER_SIZE: usize = u16::MAX as usize;

pub struct BufReader {
    pub buf: [u8; BUFFER_SIZE],
    pub big_buf: Vec<u8>,
    pub reader: OwnedReadHalf,
    pub read_offset: usize,
    pub write_offset: usize,
    pub request_size: usize,
}

impl BufReader {
    pub fn new(reader: OwnedReadHalf) -> Self {
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

pub struct BufConnection {
    pub reader: BufReader,
    pub writer: BufWriter<OwnedWriteHalf>,
}

impl<'a> BufConnection {
    pub fn new(mut connection: TcpStream) -> Self {
        let (read_half, write_half) = connection.into_split();
        let reader = BufReader::new(read_half);
        let writer = BufWriter::with_capacity(BUFFER_SIZE, write_half);

        Self {
            reader,
            writer,
        }
    }

    #[inline(always)]
    pub fn reader(&'a mut self) -> &'a mut BufReader {
        &mut self.reader
    }

    #[inline(always)]
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush().await
    }

    #[inline(always)]
    // TODO: check needed. We always call it with full len of message, even if we have some length in the buffer
    async fn read_more(reader: &mut BufReader, needed: usize) -> Status {
        if needed > BUFFER_SIZE {
            reader.big_buf.resize(needed, 0);
            let mut read = reader.write_offset - reader.read_offset;
            reader.big_buf[0..read].copy_from_slice(&reader.buf[reader.read_offset..reader.write_offset]);
            reader.write_offset = 0;
            reader.read_offset = 0;
            loop {
                match reader.reader.read(&mut reader.big_buf[read..needed]).await {
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
                        println!("Read connection error: {:?}", e);
                        return Status::Error;
                    }
                };
            }
        }

        let mut read = 0;
        if needed > BUFFER_SIZE - reader.write_offset {
            let left = reader.write_offset - reader.read_offset;
            let buf = Vec::from(&reader.buf[reader.read_offset..reader.write_offset]);
            reader.buf[0..left].copy_from_slice(&buf);
            reader.read_offset = 0;
            reader.write_offset = left;
            read += left;
        }

        loop {
            match reader.reader.read(&mut reader.buf[reader.write_offset..]).await {
                Ok(0) => {
                    return Status::Closed;
                }
                Ok(size) => {
                    reader.write_offset += size;
                    read += size;
                    if read >= needed {
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
    pub async fn read_request(&mut self) -> (Status, usize) {
        let mut reader = &mut self.reader;
        reader.write_offset = 0;
        let status = Self::read_more(&mut reader, 8).await;
        if status != Status::Ok {
            return (status, 0);
        }
        let shard_number = uint::u32(&reader.buf[..4]) as usize;
        reader.request_size = uint::u32(&reader.buf[4..8]) as usize - 8;
        reader.read_offset = 8;
        return (Status::Ok, shard_number);
    }

    #[inline(always)]
    pub async fn read_message(&mut self) -> (&'a [u8], Status) {
        if self.reader.big_buf.capacity() > 0 {
            self.reader.big_buf = Vec::with_capacity(0);
        }
        if self.reader.request_size == 0 {
            return (&[], Status::All);
        }
        let mut reader = &mut self.reader;
        if reader.write_offset < reader.read_offset + 2 {
            let status = Self::read_more(&mut reader, 2).await;
            if status != Status::Ok {
                return (&[], status);
            }
        }

        let mut len = u16(&reader.buf[reader.read_offset..reader.read_offset + 2]) as usize;
        reader.read_offset += 2;
        reader.request_size -= 2;
        if len == u16::MAX as usize {
            if reader.write_offset < reader.read_offset + 4 {
                let status = Self::read_more(&mut reader, 4).await;
                if status != Status::Ok {
                    return (&[], status);
                }
            }
            len = uint::u32(&reader.buf[reader.read_offset..reader.read_offset + 4]) as usize;
            reader.read_offset += 4;
            reader.request_size -= 4;
        }

        if reader.write_offset < reader.read_offset + len {
            let status = Self::read_more(&mut reader, len).await;
            if status != Status::Ok {
                return (&[], status);
            }
        }

        reader.request_size -= len;
        if len < u16::MAX as usize {
            reader.read_offset += len;
            let ptr = &reader.buf[reader.read_offset - len..reader.read_offset];
            return (unsafe {std::mem::transmute::<&[u8], &'a [u8]>(ptr)}, Status::Ok);
        }

        let prt = &reader.big_buf[..len];
        return (unsafe {std::mem::transmute::<&[u8], &'a [u8]>(prt)}, Status::Ok);
    }

    #[inline(always)]
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Status {
        let reader = &mut self.reader;
        if reader.write_offset < reader.read_offset + buf.len() {
            let status = Self::read_more(reader, buf.len()).await;
            if status != Status::Ok {
                return status;
            }
        }

        reader.read_offset += buf.len();
        if reader.request_size != 0 {
            reader.request_size -= buf.len();
        }

        buf.copy_from_slice(&reader.buf[reader.read_offset - buf.len()..reader.read_offset]);
        Status::Ok
    }

    #[inline(always)]
    pub fn writer(&'a mut self) -> &'a mut BufWriter<OwnedWriteHalf> {
        &mut self.writer
    }

    #[inline(always)]
    pub async fn write_message(&mut self, message: &[u8]) -> Status {
        let message_len = message.len();
        let mut res;
        if message_len < u16::MAX as usize {
            res = self.writer.write_all(&[message_len as u8, (message_len >> 8) as u8]).await;
        } else {
            res = self.writer.write_all(&[255, 255, message_len as u8, (message_len >> 8) as u8, (message_len >> 16) as u8, (message_len >> 24) as u8]).await;
        }

        if res.is_err() {
            return Status::Error;
        }

        res = self.writer.write_all(message).await;
        if res.is_err() {
            return Status::Error;
        }
        return Status::Ok;
    }

    #[inline(always)]
    pub async fn write_message_and_status(&mut self, message: &[u8], status: u8) -> Status {
        let message_len = message.len() + 1;
        let mut res;
        if message_len < u16::MAX as usize {
            res = self.writer.write_all(&[message_len as u8, (message_len >> 8) as u8]).await;
        } else {
            res = self.writer.write_all(&[255, 255, message_len as u8, (message_len >> 8) as u8, (message_len >> 16) as u8, (message_len >> 24) as u8]).await;
        }

        if res.is_err() {
            return Status::Error;
        }

        res = self.writer.write_all(&[status]).await;
        if res.is_err() {
            return Status::Error;
        }

        res = self.writer.write_all(message).await;
        if res.is_err() {
            return Status::Error;
        }
        return Status::Ok;
    }

    #[inline(always)]
    pub fn close(&mut self) -> std::io::Result<()> {Ok(())}
}

impl Drop for BufConnection {
    fn drop(&mut self) {
        self.close().expect("Failed to close connection");
    }
}

unsafe impl<'a> Sync for BufConnection {}
unsafe impl<'a> Send for BufConnection {}