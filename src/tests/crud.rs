#![cfg(test)]
use std::sync::Arc;
use std::thread;
use crate::bin_types::{BinKey, BinValue};
use crate::index::HashInMemoryIndex;
use crate::scheme::scheme::empty_scheme;
use crate::storage::Storage;
use crate::success;
use crate::writers::LogWriter;

#[cfg(test)]
pub fn crud(storage: &'static Storage) {
    let number = Storage::create_in_memory_table(storage.clone(), "crud".to_string(), HashInMemoryIndex::new(), false, empty_scheme(), &[]);
    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);

    const N: usize = 10_400_000;
    const PAR: usize = 256;
    const COUNT: usize = N / PAR;
    let mut joins = Vec::with_capacity(PAR);

    for i in 0..N {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    let keys = Arc::new(keys);
    let values = Arc::new(values);

    for i in 0..PAR {
        let i = i;
        let keys = keys.clone();
        let values = values.clone();
        let storage = storage.clone();
        let mut log_writer = LogWriter::new(storage.log_file.clone());
        joins.push(thread::spawn(move || {
            for j in 0..COUNT {
                unsafe {
                    (*storage.tables.get())[number].insert(keys[i * COUNT + j].clone(), values[i * COUNT + j].clone(), &mut log_writer);
                }
            }
        }));
    }

    for join in joins.into_iter() {
        join.join().unwrap();
    }

    let mut joins = Vec::with_capacity(PAR);
    for i in 0..PAR {
        let i = i;
        let keys = keys.clone();
        let values = values.clone();
        let storage = storage.clone();

        joins.push(thread::spawn(move || {
            for j in 0..COUNT {
                unsafe {
                    assert_eq!(values[i * COUNT + j].clone(), (*storage.tables.get())[number].get(&keys[i * COUNT + j]).unwrap());
                }
            }
        }));
    }

    for join in joins.into_iter() {
        join.join().unwrap();
    }

    success!("crud: get and insert were successful");

    let mut joins = Vec::with_capacity(PAR);
    for i in 0..PAR {
        let i = i;
        let keys = keys.clone();
        let values = values.clone();
        let storage = storage.clone();
        let mut log_writer = LogWriter::new(storage.log_file.clone());
        joins.push(thread::spawn(move || {
            for j in 0..COUNT {
                unsafe {
                    (*storage.tables.get())[number].delete(&keys[i * COUNT + j].clone(), &mut log_writer);
                }
            }
        }));
    }

    for join in joins.into_iter() {
        join.join().unwrap();
    }

    let mut joins = Vec::with_capacity(PAR);
    for i in 0..PAR {
        let i = i;
        let keys = keys.clone();
        let values = values.clone();
        let storage = storage.clone();
        joins.push(thread::spawn(move || {
            for j in 0..COUNT {
                unsafe {
                    assert_eq!(None, (*storage.tables.get())[number].get(&keys[i * COUNT + j]));
                }
            }
        }));
    }

    for join in joins.into_iter() {
        join.join().unwrap();
    }

    success!("crud: delete was successful");

    let mut joins = Vec::with_capacity(PAR);
    for i in 0..PAR {
        let i = i;
        let keys = keys.clone();
        let values = values.clone();
        let storage = storage.clone();
        let mut log_writer = LogWriter::new(storage.log_file.clone());
        joins.push(thread::spawn(move || {
            for j in 0..COUNT {
                unsafe {
                    (*storage.tables.get())[number].set(keys[i * COUNT + j].clone(), values[i * COUNT + j].clone(), &mut log_writer);
                }
            }
        }));
    }

    for join in joins.into_iter() {
        join.join().unwrap();
    }

    let mut joins = Vec::with_capacity(PAR);
    for i in 0..PAR {
        let i = i;
        let keys = keys.clone();
        let values = values.clone();
        let storage = storage.clone();
        joins.push(thread::spawn(move || {
            for j in 0..COUNT {
                unsafe {
                    assert_eq!(values[i * COUNT + j].clone(), (*storage.tables.get())[number].get(&keys[i * COUNT + j]).unwrap());
                }
            }
        }));
    }

    for join in joins.into_iter() {
        join.join().unwrap();
    }

    success!("crud: set was successful");
}