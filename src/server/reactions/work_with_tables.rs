use std::sync::Arc;
use crate::bin_types::{BinKey, BinValue};
use crate::connection::{BufConnection, Status};
use crate::constants::actions;
use crate::storage::storage::Storage;
use crate::stream::Stream;
use crate::utils::fastbytes::uint;
use crate::writers::LogWriter;

#[inline(always)]
pub fn get<S: Stream>(connection: &mut BufConnection<S>, storage: &Arc<Storage>, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            let res = table.get(&BinKey::new(&message[3..]));
            if res.is_none() {
                connection.write_message(&[actions::NOT_FOUND]);
            }
            let value = unsafe { res.unwrap_unchecked() };
            connection.write_message_and_status(value.deref(), actions::DONE)
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND])
        }
    };
}

#[inline(always)]
pub fn get_field<S: Stream>(connection: &mut BufConnection<S>, storage: &Arc<Storage>, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            let field = uint::u16(&message[3..5]);
            let res = table.get_field(&BinKey::new(&message[5..]), field as usize);
            if res.is_none() {
                return connection.write_message(&[actions::NOT_FOUND]);
            }
            let value = unsafe { res.unwrap_unchecked() };
            connection.write_message_and_status(&value, actions::DONE)
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND])
        }
    };
}

#[inline(always)]
pub fn get_fields<S: Stream>(connection: &mut BufConnection<S>, storage: &Arc<Storage>, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            let number_of_fields = uint::u16(&message[3..5]) as usize;
            let mut fields = Vec::with_capacity(number_of_fields);
            for i in 0..number_of_fields {
                fields.push(uint::u16(&message[5+i*2..5+i*2+2]) as usize);
            }
            let res = table.get_fields(&BinKey::new(&message[5..]), &fields);
            if res.is_none() {
                return connection.write_message(&[actions::NOT_FOUND])
            }
            let value = unsafe { res.unwrap_unchecked() };
            connection.write_message_and_status(&value, actions::DONE)
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND])
        }
    };
}

#[inline(always)]
pub fn insert<S: Stream>(connection: &mut BufConnection<S>, storage: &Arc<Storage>, message: &[u8], log_writer: &mut LogWriter) -> Status {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = &message[5..5+key_size];
    let value = &message[5+key_size..];
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.insert(BinKey::new(key), BinValue::new(value), log_writer);
            connection.write_message(&[actions::DONE])
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND])
        }
    };
}

#[inline(always)]
pub fn set<S: Stream>(connection: &mut BufConnection<S>, storage: &Arc<Storage>, message: &[u8], log_writer: &mut LogWriter) -> Status {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = &message[5..5+key_size];
    let value = &message[5+key_size..];
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.set(BinKey::new(key), BinValue::new(value), log_writer);
            connection.write_message(&[actions::DONE])
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND])
        }
    };
}

#[inline(always)]
pub fn delete<S: Stream>(connection: &mut BufConnection<S>, storage: &Arc<Storage>, message: &[u8], log_writer: &mut LogWriter) -> Status {
    let tables;
    unsafe {
        tables = &*storage.tables.get();
    }
    let key = &message[3..];
    return match tables.get(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.delete(&BinKey::new(key), log_writer);
            connection.write_message(&[actions::DONE])
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND])
        }
    };
}