use crate::constants::actions;
use crate::server::server::write_msg;
use crate::server::stream_trait::Stream;

#[inline(always)]
pub(crate) fn ping(stream: &mut impl Stream, write_buf: &mut [u8], write_offset: usize) -> usize {
    write_msg(stream, write_buf, write_offset, &[actions::DONE, actions::PING])
}