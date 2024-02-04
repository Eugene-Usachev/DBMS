use crate::bin_types::{BinKey, BinValue};
use crate::connection::{BufConnection, Status};
use crate::constants::actions;
use crate::storage::storage::Storage;
use crate::utils::fastbytes::uint;

#[inline(always)]
pub async fn get(connection: &mut BufConnection, storage: &mut Storage, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = storage.tables.get_mut();
    }
    return match tables.get_mut(uint::u16(&message[1..3]) as usize) {
        Some(table) => unsafe {
            let res = table.get(&BinKey::new(&message[3..]));
            if res.is_none() {
                connection.write_message(&[actions::NOT_FOUND]).await;
            }
            let value = res.unwrap_unchecked();
            connection.write_message_and_status(value.deref(), actions::DONE).await
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND]).await
        }
    };
}

#[inline(always)]
pub async fn get_field(connection: &mut BufConnection, storage: &mut Storage, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = storage.tables.get_mut();
    }
    println!("{}, but len is {}", uint::u16(&message[1..3]), tables.len());
    return match tables.get_mut(uint::u16(&message[1..3]) as usize) {
        Some(table) => unsafe {
            let field = uint::u16(&message[3..5]);
            let res = table.get_field(&BinKey::new(&message[5..]), field as usize);
            if res.is_none() {
                return connection.write_message(&[actions::NOT_FOUND]).await;
            }
            let value = res.unwrap_unchecked();
            connection.write_message_and_status(&value, actions::DONE).await
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND]).await
        }
    };
}

#[inline(always)]
pub async fn get_fields(connection: &mut BufConnection, storage: &mut Storage, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = storage.tables.get_mut();
    }
    return match tables.get_mut(uint::u16(&message[1..3]) as usize) {
        Some(table) => unsafe {
            let number_of_fields = uint::u16(&message[3..5]) as usize;
            let mut fields = Vec::with_capacity(number_of_fields);
            for i in 0..number_of_fields {
                fields.push(uint::u16(&message[5+i*2..5+i*2+2]) as usize);
            }
            let res = table.get_fields(&BinKey::new(&message[5..]), &fields);
            if res.is_none() {
                return connection.write_message(&[actions::NOT_FOUND]).await
            }
            let value = res.unwrap_unchecked();
            connection.write_message_and_status(&value, actions::DONE).await
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND]).await
        }
    };
}

#[inline(always)]
pub async fn insert(connection: &mut BufConnection, storage: &mut Storage, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = storage.tables.get_mut();
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = &message[5..5+key_size];
    let value = &message[5+key_size..];
    return match tables.get_mut(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.insert(BinKey::new(key), BinValue::new(value), &mut storage.log_writer);
            connection.write_message(&[actions::DONE]).await
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND]).await
        }
    };
}

#[inline(always)]
pub async fn set(connection: &mut BufConnection, storage: &mut Storage, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = storage.tables.get_mut();
    }
    let key_size = uint::u16(&message[3..5]) as usize;
    let key = &message[5..5+key_size];
    let value = &message[5+key_size..];
    return match tables.get_mut(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.set(BinKey::new(key), BinValue::new(value), &mut storage.log_writer);
            connection.write_message(&[actions::DONE]).await
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND]).await
        }
    };
}

#[inline(always)]
pub async fn delete(connection: &mut BufConnection, storage: &mut Storage, message: &[u8]) -> Status {
    let tables;
    unsafe {
        tables = storage.tables.get_mut();
    }
    let key = &message[3..];
    return match tables.get_mut(uint::u16(&message[1..3]) as usize) {
        Some(table) => {
            table.delete(&BinKey::new(key), &mut storage.log_writer);
            connection.write_message(&[actions::DONE]).await
        }
        None => {
           connection.write_message(&[actions::TABLE_NOT_FOUND]).await
        }
    };
}