use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use crate::bin_types::{BinKey, BinValue};
use crate::index::HashInMemoryIndex;
use crate::storage::Storage;
use crate::table::in_memory::InMemoryTable;
use crate::tests::TABLES_CREATED;

pub fn crud(storage: Arc<Storage>) {
    let number = TABLES_CREATED.fetch_add(1, SeqCst);
    storage.tables.write().unwrap().push(Box::new(InMemoryTable::new(
        number as u16, HashInMemoryIndex::new(), "crud".to_string(), false, 0
    )));
    let mut keys = Vec::with_capacity(10000);
    let mut values = Vec::with_capacity(10000);
    for i in 0..10000 {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }

    for i in 0..10000 {
        storage.tables.write().unwrap()[number].insert(keys[i].clone(), values[i].clone(), &mut [], &mut 0);
    }

    for i in 0..10000 {
        assert_eq!(values[i].clone(), storage.tables.write().unwrap()[number].get(&keys[i]).unwrap());
    }

    println!("crud: get and insert were successful");

    for i in 0..10000 {
        storage.tables.write().unwrap()[number].delete(&keys[i], &mut [], &mut 0);
    }

    for i in 0..10000 {
        assert_eq!(None, storage.tables.write().unwrap()[number].get(&keys[i]));
    }

    println!("crud: delete was successful");

    for i in 0..10000 {
        storage.tables.write().unwrap()[number].set(keys[i].clone(), values[i].clone(), &mut [], &mut 0);
    }

    for i in 0..10000 {
        assert_eq!(values[i].clone(), storage.tables.write().unwrap()[number].get(&keys[i]).unwrap());
    }

    println!("crud: set was successful");
}