use std::marker::PhantomData;
use crate::stream::Stream;
use crate::connection::{
    status::Status,
    reader::BufReader as BReader,
    writer::BufWriter as BWriter,
    BUFFER_SIZE
};

pub trait BufReader<'stream, S: Stream> {
    fn read_more(&mut self, needed: usize) -> Status;
    /// read request returns status and is a request reading.
    fn read_request(&mut self) -> (Status, bool);
    fn read_message(&mut self) -> (&'stream [u8], Status);
}

pub trait BufWriter<'stream, S: Stream> {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()>;
    fn stream<'a>(&'a mut self) -> &'a mut S;
    fn flush(&mut self) -> std::io::Result<()>;
    fn write_message(&mut self, message: &[u8]) -> Status;
    fn write_message_and_status(&mut self, message: &[u8], status: u8) -> Status;
    fn close(&mut self) -> std::io::Result<()>;
}

pub fn split_buffered<S: Stream>(stream: S) -> (BReader<S>, BWriter<S>) {
    let clone = stream.clone_ptr();
    let reader = BReader::new(clone);
    let writer = BWriter::new(stream);
    (reader, writer)
}

pub struct BufConnection<'stream, S: Stream, R: BufReader<'stream, S>, W: BufWriter<'stream, S>> {
    pub reader: R,
    pub writer: W,

    pd: PhantomData<&'stream S>
}

impl<'stream, S: Stream, R: BufReader<'stream, S>, W: BufWriter<'stream, S>> BufConnection<'stream, S, R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader,
            writer,
            pd: PhantomData,
        }
    }

    #[inline(always)]
    #[allow(unused)]
    pub fn reader(&'stream mut self) -> &'stream mut R {
        &mut self.reader
    }

    #[inline(always)]
    #[allow(unused)]
    pub fn stream(&'stream mut self) -> &'stream mut S {
        self.writer.stream()
    }

    #[inline(always)]
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    #[inline(always)]
    pub fn read_request(&mut self) -> (Status, bool) {
        self.reader.read_request()
    }

    #[inline(always)]
    pub fn read_message(&mut self) -> (&[u8], Status) {
        self.reader.read_message()
    }

    #[inline(always)]
    pub fn write_message(&mut self, message: &[u8]) -> Status {
        self.writer.write_message(message)
    }

    #[inline(always)]
    pub fn write_message_and_status(&mut self, message: &[u8], status: u8) -> Status {
        self.writer.write_message_and_status(message, status)
    }

    #[inline(always)]
    pub fn close(&mut self) -> std::io::Result<()> {
        self.writer.close()
    }
}

pub fn buffered<'stream, S: Stream>(stream: S) -> BufConnection<'stream, S, BReader<S>, BWriter<S>> {
    let (reader, writer) = split_buffered(stream);
    BufConnection::new(reader, writer)
}