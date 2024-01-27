pub const DONE: u8 = 0u8;
pub const BAD_REQUEST: u8 = 1u8;
pub const INTERNAL_ERROR: u8 = 2u8;
pub const TABLE_NOT_FOUND: u8 = 3u8;
pub const NOT_FOUND: u8 = 4u8;

pub const CREATE_TABLE_IN_MEMORY: u8 = 5u8;
pub const CREATE_TABLE_CACHE: u8 = 6u8;

pub const CREATE_TABLE_ON_DISK: u8 = 7u8;
pub const GET_TABLES_NAMES: u8 = 8u8;

pub const PING: u8 = 9u8;
pub const GET_SHARD_METADATA: u8 = 10u8;
pub const GET_HIERARCHY: u8 = 11u8;

pub const GET: u8 = 12u8;
pub const GET_FIELD: u8 = 13u8;
pub const GET_FIELDS: u8 = 14u8;
pub const INSERT: u8 = 15u8;
pub const SET: u8 = 16u8;
pub const DELETE: u8 = 17u8;

/// If the number of action is greater than 255, you need to use big action and add the action number after (like [255u8, 1u8, 254u8])
#[allow(dead_code)]
pub const BIG_ACTION: u8 = 255u8;