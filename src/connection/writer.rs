use crate::{
    connection::{
        connection::BufWriter as BufWriterTrait,
        BUFFER_SIZE,
        status::Status
    },
    stream::Stream
};

pub struct BufWriter<S: Stream> {
    buf: [u8; BUFFER_SIZE],
    offset: usize,
    writer: S,
}

impl<S: Stream> BufWriter<S> {
    pub fn new(writer: S) -> BufWriter<S> {
        Self {
            buf: [0u8; BUFFER_SIZE],
            offset: 0,
            writer
        }
    }

    fn flush_buf(&mut self) -> std::io::Result<()> {
        Stream::write_all(&mut self.writer, &self.buf[..self.offset])?;
        self.offset = 0;
        Ok(())
    }
}

impl<'stream, S: Stream> BufWriterTrait<'stream, S> for BufWriter<S> {

    #[inline(always)]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let len = buf.len();
        let needed = self.offset + len;
        if needed > BUFFER_SIZE {
            self.flush_buf()?;

            if len <= BUFFER_SIZE {
                self.buf[..len].copy_from_slice(buf);
                self.offset = len;
                return Ok(());
            }
            return Stream::write_all(&mut self.writer, buf);
        }

        self.buf[self.offset..needed].copy_from_slice(buf);
        self.offset = needed;

        Ok(())
    }
    fn stream<'a>(&'a mut self) -> &'a mut S {
        &mut self.writer
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        if self.offset > 0 {
            self.flush_buf()?;
        }
        Ok(())
    }

    fn write_message(&mut self, message: &[u8]) -> Status {
        let message_len = message.len();
        let mut res;
        if message_len < u16::MAX as usize {
            res = self.write_all(&[message_len as u8, (message_len >> 8) as u8]);
        } else {
            res = self.write_all(&[255, 255, message_len as u8, (message_len >> 8) as u8, (message_len >> 16) as u8, (message_len >> 24) as u8]);
        }

        if res.is_err() {
            return Status::Error;
        }

        res = self.write_all(message);
        if res.is_err() {
            return Status::Error;
        }
        return Status::Ok;
    }

    fn write_message_and_status(&mut self, message: &[u8], status: u8) -> Status {
        let message_len = message.len() + 1;
        let mut res;
        if message_len < u16::MAX as usize {
            res = self.write_all(&[message_len as u8, (message_len >> 8) as u8]);
        } else {
            res = self.write_all(&[255, 255, message_len as u8, (message_len >> 8) as u8, (message_len >> 16) as u8, (message_len >> 24) as u8]);
        }

        if res.is_err() {
            return Status::Error;
        }

        res = self.write_all(&[status]);
        if res.is_err() {
            return Status::Error;
        }

        res = self.write_all(message);
        if res.is_err() {
            return Status::Error;
        }
        return Status::Ok;
    }

    fn close(&mut self) -> std::io::Result<()> {
        self.flush()?;
        self.writer.shutdown()
    }
}
