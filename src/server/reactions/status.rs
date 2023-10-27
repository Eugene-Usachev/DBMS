use std::net::TcpStream;
use crate::constants::actions;
use crate::server::server::write_msg;

#[inline(always)]
pub(crate) fn ping(stream: &mut TcpStream, write_buf: &mut [u8], write_offset: usize) -> usize {
    write_msg(stream, write_buf, write_offset, &[actions::PING])
}