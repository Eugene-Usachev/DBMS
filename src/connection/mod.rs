pub mod connection;
mod writer;
mod reader;
pub mod status;

const BUFFER_SIZE: usize = u16::MAX as usize;

pub use connection::*;
pub use status::Status;