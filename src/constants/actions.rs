pub const DONE: u8 = 0u8;
pub const BAD_REQUEST: u8 = 1u8;
pub const INTERNAL_ERROR: u8 = 2u8;
pub const SPACE_NOT_FOUND: u8 = 3u8;
pub const NOT_FOUND: u8 = 4u8;

pub const CREATE_SPACE_IN_MEMORY: u8 = 5u8;
pub const CREATE_SPACE_CACHE: u8 = 6u8;

pub const CREATE_SPACE_ON_DISK: u8 = 7u8;
pub const GET_SPACES_NAMES: u8 = 8u8;

pub const PING: u8 = 9u8;
pub const GET: u8 = 10u8;
pub const INSERT: u8 = 11u8;
pub const SET: u8 = 12u8;
pub const DELETE: u8 = 13u8;

pub const GET_AND_RESET_CACHE_TIME: u8 = 14u8;

/// If the number of action is greater than 255, you need to use big action and add the action number after (like [255u8, 1u8, 254u8])
pub const BIG_ACTION: u8 = 255u8;