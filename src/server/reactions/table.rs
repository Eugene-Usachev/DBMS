use crate::connection::{BufConnection, Status};
use crate::constants::actions;
use crate::index::HashInMemoryIndex;
use crate::scheme::scheme::{empty_scheme, scheme_from_bytes};
use crate::storage::storage::Storage;
use crate::stream::Stream;
use crate::utils::fastbytes::uint;
use crate::writers::{LogWriter};

#[inline(always)]
pub fn create_table_in_memory<S: Stream>(connection: &mut BufConnection<S>, storage: &'static Storage, message: &[u8], log_writer: &mut LogWriter) -> Status {
    if message.len() < 7 {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    let is_it_logging = message[1] != 0;
    let scheme_len = ((message[3] as u16) << 8 | message[2] as u16) as usize;
    if scheme_len + 4 + 2 > message.len() {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    let user_scheme: &[u8];
    let scheme;
    if scheme_len == 0 {
        user_scheme = &[];
        scheme = Ok(empty_scheme());
    } else {
        user_scheme = &message[4..4 + scheme_len];
        scheme = scheme_from_bytes(user_scheme);
        if scheme.is_err() {
            return connection.write_message(&[actions::BAD_REQUEST]);
        }
    }

    let name = String::from_utf8(message[4 + scheme_len..].to_vec()).unwrap();
    let name_len = name.len();
    {
        let mut buf = vec![0; name_len + 6 + scheme_len];
        buf[0] = actions::CREATE_TABLE_IN_MEMORY;
        buf[1] = name_len as u8;
        buf[2] = (name_len >> 8) as u8;
        buf[3] = if is_it_logging { 1 } else { 0 };
        let mut offset = 4;
        buf[offset..offset + name_len].copy_from_slice(name.as_bytes());
        offset += name_len;
        buf[offset] = scheme_len as u8;
        offset += 1;
        buf[offset] = (scheme_len >> 8) as u8;
        offset += 1;
        buf[offset..].copy_from_slice(user_scheme);
        log_writer.write_slice(&buf);
    }


    let l = Storage::create_in_memory_table(storage, name, HashInMemoryIndex::new(), is_it_logging, scheme.unwrap(), user_scheme);
    if l == (u16::MAX - 1u16) as usize {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    
    
    connection.write_message(&[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_table_on_disk<S: Stream>(connection: &mut BufConnection<S>, storage: &'static Storage, message: &[u8],  log_writer: &mut LogWriter) -> Status {
    if message.len() < 6 {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    let scheme_len = ((message[2] as u16) << 8 | message[1] as u16) as usize;
    if scheme_len + 4 + 2 > message.len() {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    let user_scheme: &[u8];
    let scheme;
    if scheme_len == 0 {
        user_scheme = &[];
        scheme = Ok(empty_scheme());
    } else {
        user_scheme = &message[3..3 + scheme_len];
        scheme = scheme_from_bytes(user_scheme);
        if scheme.is_err() {
            return connection.write_message(&[actions::BAD_REQUEST]);
        }
    }

    let name = String::from_utf8(message[3 + scheme_len..].to_vec()).unwrap();
    let name_len = name.len();
    {
        let mut buf = vec![0; name_len + 5 + scheme_len];
        buf[0] = actions::CREATE_TABLE_ON_DISK;
        buf[1] = name_len as u8;
        buf[2] = (name_len >> 8) as u8;
        let mut offset = 3;
        buf[offset..offset + name_len].copy_from_slice(name.as_bytes());
        offset += name_len;
        buf[offset] = scheme_len as u8;
        offset += 1;
        buf[offset] = (scheme_len >> 8) as u8;
        offset += 1;
        buf[offset..].copy_from_slice(user_scheme);
        log_writer.write_slice(&buf);
    }


    let l = Storage::create_on_disk_table(storage, name, HashInMemoryIndex::new(), scheme.unwrap(), user_scheme);
    if l == (u16::MAX - 1u16) as usize {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }


    connection.write_message(&[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn create_table_cache<S: Stream>(connection: &mut BufConnection<S>, storage: &'static Storage, message: &[u8],  log_writer: &mut LogWriter) -> Status {
    if message.len() < 11 {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    let is_it_logging = message[1] != 0;
    let cache_duration = uint::u64(&message[2..10]);
    let scheme_len = ((message[11] as u16) << 8 | message[10] as u16) as usize;
    if scheme_len + 12 + 2 > message.len() {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    let user_scheme: &[u8];
    let scheme;
    if scheme_len == 0 {
        user_scheme = &[];
        scheme = Ok(empty_scheme());
    } else {
        user_scheme = &message[12..12 + scheme_len];
        scheme = scheme_from_bytes(user_scheme);
        if scheme.is_err() {
            return connection.write_message(&[actions::BAD_REQUEST]);
        }
    }

    let name = String::from_utf8(message[12 + scheme_len..].to_vec()).unwrap();
    let name_len = name.len();
    {
        // TODO: maybe extra two bytes?
        let mut buf = vec![0; name_len + 14 + scheme_len];
        buf[0] = actions::CREATE_TABLE_CACHE;
        buf[1] = name_len as u8;
        buf[2] = (name_len >> 8) as u8;
        buf[3] = if is_it_logging { 1 } else { 0 };
        buf[4] = (cache_duration >> 56) as u8;
        buf[5] = (cache_duration >> 48) as u8;
        buf[6] = (cache_duration >> 40) as u8;
        buf[7] = (cache_duration >> 32) as u8;
        buf[8] = (cache_duration >> 24) as u8;
        buf[9] = (cache_duration >> 16) as u8;
        buf[10] = (cache_duration >> 8) as u8;
        buf[11] = cache_duration as u8;
        let mut offset = 12;
        buf[offset..offset + name_len].copy_from_slice(name.as_bytes());
        offset += name_len;
        buf[offset] = scheme_len as u8;
        offset += 1;
        buf[offset] = (scheme_len >> 8) as u8;
        offset += 1;
        buf[offset..].copy_from_slice(user_scheme);
        log_writer.write_slice(&buf);
    }

    let l = Storage::create_cache_table(storage, name, HashInMemoryIndex::new(), cache_duration, is_it_logging, scheme.unwrap(), user_scheme);
    if l == (u16::MAX - 1u16) as usize {
        return connection.write_message(&[actions::BAD_REQUEST]);
    }
    connection.write_message(&[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
}

#[inline(always)]
pub fn get_tables_names<S: Stream>(connection: &mut BufConnection<S>, storage: &'static Storage) -> Status {
    let tables_names;
    let tables_names_not_unwrapped = storage.tables_names.read();
    match tables_names_not_unwrapped {
        Ok(tables_names_unwrapped) => {
            tables_names = tables_names_unwrapped;
        }
        Err(_) => {
            return connection.write_message(&[actions::INTERNAL_ERROR]);
        }
    }

    let mut local_buffer = Vec::with_capacity(4096);
    local_buffer[0] = actions::DONE;
    for name in tables_names.iter() {
        let name_len = name.len();
        if name_len < u16::MAX as usize {
            local_buffer.extend_from_slice(&[name_len as u8, (name_len >> 8) as u8]);
        } else {
            local_buffer.extend_from_slice(&[255, 255, name_len as u8, (name_len >> 8) as u8, (name_len >> 16) as u8, (name_len >> 24) as u8]);
        }
        local_buffer.extend_from_slice(name.as_bytes());
    }

    connection.write_message(&local_buffer)
}