#[repr(u8)]
#[derive(PartialEq)]
pub enum Status {
    Ok,
    All,
    Closed,
    Error
}