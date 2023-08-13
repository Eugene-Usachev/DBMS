pub const DONE: u8 = 0u8;
pub const SPACE_NOT_FOUND: u8 = 1u8;
pub const PING: u8 = 2u8;
pub const CREATE_SPACE: u8 = 3u8;
pub const GET: u8 = 4u8;
pub const INSERT: u8 = 5u8;
/// If the number of action is greater than 255, you need to use big action and add the action number after (like [255u8, 1u8, 254u8])
pub const BIG_ACTION: u8 = 255u8;