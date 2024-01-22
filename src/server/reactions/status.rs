use crate::connection::{BufConnection, Status};
use crate::constants::actions;
use crate::stream::Stream;

#[inline(always)]
pub(crate) fn ping<S: Stream>(connection: &mut BufConnection<S>) -> Status {
    connection.write_message(&[actions::DONE, actions::PING])
}