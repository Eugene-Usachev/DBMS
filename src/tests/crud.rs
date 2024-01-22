use std::sync::Arc;
use crate::bin_types::{BinKey, BinValue};
use crate::index::HashInMemoryIndex;
use crate::scheme::scheme::empty_scheme;
use crate::storage::Storage;
use crate::writers::LogWriter;

#[cfg(test)]
pub fn crud(storage: Arc<Storage>) {
    let number = Storage::create_in_memory_table(storage.clone(), "crud".to_string(), HashInMemoryIndex::new(), false, empty_scheme(), &[]);
    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);
    let mut log_writer = LogWriter::new(storage.log_file.clone());
    for i in 0..10000 {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    for i in 0..10000 {
        unsafe {
            (*storage.tables.get())[number].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
        }

    }

    for i in 0..10000 {
        unsafe {
            assert_eq!(values[i].clone(), (*storage.tables.get())[number].get(&keys[i]).unwrap());
        }
    }

    println!("crud: get and insert were successful");

    for i in 0..10000 {
        unsafe {
            (*storage.tables.get())[number].delete(&keys[i], &mut log_writer);
        }
    }

    for i in 0..10000 {
        unsafe {
            assert_eq!(None, (*storage.tables.get())[number].get(&keys[i]));
        }
    }

    println!("crud: delete was successful");

    for i in 0..10000 {
        unsafe {
            (*storage.tables.get())[number].set(keys[i].clone(), values[i].clone(), &mut log_writer);
        }
    }

    for i in 0..10000 {
        unsafe {
            assert_eq!(values[i].clone(), (*storage.tables.get())[number].get(&keys[i]).unwrap());
        }
    }

    println!("crud: set was successful");
}