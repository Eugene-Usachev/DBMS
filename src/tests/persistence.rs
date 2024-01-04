use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use crate::bin_types::{BinKey, BinValue};
use crate::index::HashInMemoryIndex;
use crate::storage::Storage;
use crate::table::in_memory::InMemoryTable;
use crate::tests::TABLES_CREATED;

/// persistence creates two tables. It inserts data and deletes a few of them in both tables.
/// Then it starts dumps and deletes both tables. And then it creates the tables again and rises it. Then check for all data.
///
/// Next it creates new two tables and inserts and deletes data. Then it dumps and inserts and deletes some new data.
/// After it deletes both tables and creates them again. Then rises it and read the log. And check for all data.
pub fn persistence(storage: Arc<Storage>) {
    test_dump(storage.clone());
    test_dump_and_log(storage.clone());
}

fn test_dump(storage: Arc<Storage>) {
    let number1 = TABLES_CREATED.fetch_add(1, SeqCst);
    let mut tables = storage.tables.write().unwrap();
    tables.push(Box::new(InMemoryTable::new(
        number1 as u16, HashInMemoryIndex::new(), "persistence 1".to_string(), false, 0
    )));
    let number2 = TABLES_CREATED.fetch_add(1, SeqCst);
    tables.push(Box::new(InMemoryTable::new(
        number2 as u16, HashInMemoryIndex::new(), "persistence 2".to_string(), false, 0
    )));

    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);
    for i in 0..10000 {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    for i in 0..10000 {
        tables[number1].insert(keys[i].clone(), values[i].clone(), &mut [], &mut 0);
        tables[number2].insert(keys[i].clone(), values[i].clone(), &mut [], &mut 0);
    }
    for i in 0..10000 {
        if i % 2 == 0 {
            tables[number1].delete(&keys[i], &mut [], &mut 0);
        } else {
            tables[number2].delete(&keys[i], &mut [], &mut 0);
        }
    }

    tables[number1].dump();
    tables[number2].dump();

    tables.remove(number1);
    tables.remove(number2 - 1);

    tables.push(Box::new(InMemoryTable::new(
        number1 as u16, HashInMemoryIndex::new(), "persistence 1".to_string(), false, 1
    )));

    tables.push(Box::new(InMemoryTable::new(
        number2 as u16, HashInMemoryIndex::new(), "persistence 2".to_string(), false, 1
    )));

    tables[number1].rise(1);
    tables[number2].rise(1);

    for i in 0..10000 {
        if i % 2 == 0 {
            assert_eq!(None, tables[number1].get(&keys[i]));
            assert_eq!(values[i].clone(), tables[number2].get(&keys[i]).unwrap());
        } else {
            assert_eq!(values[i].clone(), tables[number1].get(&keys[i]).unwrap());
            assert_eq!(None, tables[number2].get(&keys[i]));
        }
    }

    println!("persistence: dump was successful");
}

fn test_dump_and_log(storage: Arc<Storage>) {
    let number1 = TABLES_CREATED.fetch_add(1, SeqCst);
    let mut tables = storage.tables.write().unwrap();
    tables.push(Box::new(InMemoryTable::new(
        number1 as u16, HashInMemoryIndex::new(), "persistence 3".to_string(), true, 0
    )));
    let number2 = TABLES_CREATED.fetch_add(1, SeqCst);
    tables.push(Box::new(InMemoryTable::new(
        number2 as u16, HashInMemoryIndex::new(), "persistence 4".to_string(), true, 0
    )));

    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);
    for i in 0..10000 {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    let mut log_buffer = [0u8; 612 * 1024];
    let mut log_buffer_offset = 0;

    for i in 0..5000 {
        tables[number1].insert(keys[i].clone(), values[i].clone(), &mut log_buffer, &mut log_buffer_offset);
        tables[number2].insert(keys[i].clone(), values[i].clone(), &mut log_buffer, &mut log_buffer_offset);
    }
    for i in 0..5000 {
        if i % 2 == 0 {
            tables[number1].delete(&keys[i], &mut log_buffer, &mut log_buffer_offset);
        } else {
            tables[number2].delete(&keys[i], &mut log_buffer, &mut log_buffer_offset);
        }
    }

    tables[number1].dump();
    tables[number2 - 1].dump();

    for i in 5000..10000 {
        tables[number1].insert(keys[i].clone(), values[i].clone(), &mut log_buffer, &mut log_buffer_offset);
        tables[number2].insert(keys[i].clone(), values[i].clone(), &mut log_buffer, &mut log_buffer_offset);
    }
    for i in 5000..10000 {
        if i % 2 == 0 {
            tables[number1].delete(&keys[i], &mut log_buffer, &mut log_buffer_offset);
        } else {
            tables[number2].delete(&keys[i], &mut log_buffer, &mut log_buffer_offset);
        }
    }
    let mut writer = storage.log_file.file.lock().unwrap();
    writer.write_all(&log_buffer[..log_buffer_offset]).expect("failed to write log");
    writer.flush().expect("failed to flush log");

    tables.remove(number1);
    tables.remove(number2 - 1);

    tables.push(Box::new(InMemoryTable::new(
        number1 as u16, HashInMemoryIndex::new(), "persistence 3".to_string(), true, 1
    )));

    tables.push(Box::new(InMemoryTable::new(
        number2 as u16, HashInMemoryIndex::new(), "persistence 4".to_string(), true, 1
    )));

    tables[number1].rise(1);
    tables[number2].rise(1);

    Storage::read_log(storage.clone());

    for i in 0..10000 {
        if i % 2 == 0 {
            assert_eq!(None, tables[number1].get(&keys[i]));
            assert_eq!(values[i].clone(), tables[number2].get(&keys[i]).unwrap());
        } else {
            assert_eq!(values[i].clone(), tables[number1].get(&keys[i]).unwrap());
            assert_eq!(None, tables[number2].get(&keys[i]));
        }
    }

    println!("persistence: dump was successful");
}