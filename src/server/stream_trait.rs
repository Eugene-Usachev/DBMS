use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;

pub trait Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
    fn write_all(&mut self, buf: &[u8])  -> std::io::Result<()>;
}

impl Stream for TcpStream {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        <TcpStream as Read>::read(self, buf)
    }
    #[inline(always)]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        <TcpStream as Write>::write_all(self, buf)
    }
}

impl Stream for File {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        <File as Read>::read(self, buf)
    }

    #[inline(always)]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        <File as Write>::write_all(self, buf)
    }
}