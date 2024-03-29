#![cfg(test)]
use crate::{
    bin_types::{BinKey, BinValue},
    index::HashInMemoryIndex,
    scheme::scheme::empty_scheme,
    storage::Storage,
    success,
    writers::LogWriter
};

#[cfg(test)]
/// persistence creates two tables. It inserts data and deletes a few of them in both tables.
/// Then it starts dumps and deletes both tables. And then it creates the tables again and rises it. Then check for all data.
///
/// Next it creates new two tables and inserts and deletes data. Then it dumps and inserts and deletes some new data.
/// After it deletes both tables and creates them again. Then rises it and read the log. And check for all data.
pub fn persistence(storage: &'static Storage) {
    test_dump(storage);
    test_dump_and_log(storage);
}

#[cfg(test)]
static SCHEMA: &'static [u8] = br#"{
    "sized_fields": {
        "key": "Uint32"
    },
    "unsized_fields": {
        "key2": "String"
    }
}"#;

#[cfg(test)]
fn test_dump(storage: &'static Storage) {
    let number1 = Storage::create_in_memory_table(storage.clone(), "persistence 1".to_string(), HashInMemoryIndex::new(), false, empty_scheme(), SCHEMA);
    let number2 = Storage::create_in_memory_table(storage.clone(), "persistence 2".to_string(), HashInMemoryIndex::new(), false, empty_scheme(), SCHEMA);
    let tables;
    let mut log_writer = LogWriter::new(storage.log_file.clone());
    unsafe {
        tables = &*storage.tables.get()
    };


    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);
    for i in 0..10000 {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    for i in 0..10000 {
        tables[number1].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
        tables[number2].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
    }
    for i in 0..10000 {
        if i % 2 == 0 {
            tables[number1].delete(&keys[i], &mut log_writer);
        } else {
            tables[number2].delete(&keys[i], &mut log_writer);
        }
    }

    Storage::dump(storage.clone());
    let tables = storage.tables.get_mut();

    tables.remove(number1);
    tables.remove(number2 - 1);
    let mut tables_names = storage.tables_names.write().unwrap();
    tables_names.remove(number1);
    tables_names.remove(number2 - 1);
    drop(tables_names);

    Storage::rise(storage.clone());

    let count = tables[number1].count();
    if count != 5000 {
        panic!("count: {}", count);
    }
    let count = tables[number2].count();
    if count != 5000 {
        panic!("count: {}", count);
    }

    for i in 0..10000 {
        if i % 2 == 0 {
            assert_eq!(None, tables[number1].get(&keys[i]));
            assert_eq!(values[i].clone(), tables[number2].get(&keys[i]).unwrap());
        } else {
            assert_eq!(values[i].clone(), tables[number1].get(&keys[i]).unwrap());
            assert_eq!(None, tables[number2].get(&keys[i]));
        }
    }

    if !tables[number1].user_scheme()[..].eq(SCHEMA) {
        panic!("can't read after rise. scheme: {:?}", tables[number1].user_scheme());
    }
    if !tables[number2].user_scheme()[..].eq(SCHEMA) {
        panic!("can't read after rise. scheme: {:?}", tables[number2].user_scheme());
    }

    success!("persistence: dump was successful");
}

#[cfg(test)]
fn test_dump_and_log(storage: &'static Storage) {
    let number1 = Storage::create_in_memory_table(storage.clone(), "persistence 3".to_string(), HashInMemoryIndex::new(), true, empty_scheme(), SCHEMA);
    let number2 = Storage::create_in_memory_table(storage.clone(), "persistence 4".to_string(), HashInMemoryIndex::new(), true, empty_scheme(), SCHEMA);
    let tables = storage.tables.get_mut();

    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);
    for i in 0..10000 {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    let mut log_writer = LogWriter::new(storage.log_file.clone());

    for i in 0..5000 {
        tables[number1].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
        tables[number2].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
    }
    for i in 0..5000 {
        if i % 2 == 0 {
            tables[number1].delete(&keys[i], &mut log_writer);
        } else {
            tables[number2].delete(&keys[i], &mut log_writer);
        }
    }

    Storage::dump(storage.clone());

    for i in 5000..10000 {
        tables[number1].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
        tables[number2].insert(keys[i].clone(), values[i].clone(), &mut log_writer);
    }
    for i in 5000..10000 {
        if i % 2 == 0 {
            tables[number1].delete(&keys[i], &mut log_writer);
        } else {
            tables[number2].delete(&keys[i], &mut log_writer);
        }
    }
    log_writer.flush();

    tables.remove(number1);
    tables.remove(number2 - 1);
    let mut tables_names = storage.tables_names.write().unwrap();
    tables_names.remove(number1);
    tables_names.remove(number2 - 1);
    drop(tables_names);

    Storage::rise(storage.clone());

    let count = tables[number1].count();
    if count != 5000 {
        panic!("count: {}", count);
    }
    let count = tables[number2].count();
    if count != 5000 {
        panic!("count: {}", count);
    }

    for i in 0..10000 {
        if i % 2 == 0 {
            assert_eq!(None, tables[number1].get(&keys[i]));
            assert_eq!(values[i].clone(), tables[number2].get(&keys[i]).unwrap());
        } else {
            assert_eq!(values[i].clone(), tables[number1].get(&keys[i]).unwrap());
            assert_eq!(None, tables[number2].get(&keys[i]));
        }
    }

    success!("persistence: dump and read the log was successful");
}