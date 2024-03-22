use std::{
    io::{Read, Write},
    net::{Shutdown, TcpStream},
    fs::File,
    time::Duration
};
#[cfg(not(target_os = "windows"))]
use std::os::unix::net::UnixStream;

pub trait Stream: Read + Write {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
    fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> std::io::Result<()>;
    fn write_all(&mut self, buf: &[u8])  -> std::io::Result<()>;
    fn shutdown(&mut self) -> std::io::Result<()>;
    fn clone_ptr<'a>(&self) -> Self;
}

#[cfg(not(target_os = "windows"))]
impl Stream for UnixStream {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let res = <UnixStream as Read>::read(self, buf);
            return match res {
                Ok(n) => {
                    Ok(n)
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    Err(e)
                }
            }
        }
    }

    #[inline(always)]
    fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> std::io::Result<()> {
        UnixStream::set_read_timeout(self, timeout)
    }

    #[inline(always)]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        <UnixStream as Write>::write_all(self, buf)
    }

    #[inline(always)]
    fn shutdown(&mut self) -> std::io::Result<()> {
        UnixStream::shutdown(self, Shutdown::Both)
    }

    #[inline(always)]
    fn clone_ptr<'a>(&self) -> Self {
        self.try_clone().unwrap()
    }
}

impl Stream for TcpStream {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let res = <TcpStream as Read>::read(self, buf);
            return match res {
                Ok(n) => {
                    Ok(n)
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    Err(e)
                }
            }
        }
    }

    #[inline(always)]
    fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    #[inline(always)]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        <TcpStream as Write>::write_all(self, buf)
    }

    #[inline(always)]
    fn shutdown(&mut self) -> std::io::Result<()> {
        TcpStream::shutdown(self, Shutdown::Both)
    }

    #[inline(always)]
    fn clone_ptr<'a>(&self) -> Self {
        self.try_clone().unwrap()
    }
}

impl Stream for File {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        <File as Read>::read(self, buf)
    }

    #[inline(always)]
    fn set_read_timeout(&self, _timeout: Option<Duration>) -> std::io::Result<()> {
        return Ok(());
    }

    #[inline(always)]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        <File as Write>::write_all(self, buf)
    }

    #[allow(unused_must_use)]
    #[inline(always)]
    fn shutdown(&mut self) -> std::io::Result<()> {
        <File as Write>::flush(self)
    }

    #[inline(always)]
    fn clone_ptr<'a>(&self) -> Self {
        self.try_clone().unwrap()
    }
}