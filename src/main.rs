#![feature(core_intrinsics)]
use std::sync::Arc;

mod index;
mod bin_types;
mod constants;
mod utils;
mod storage;

use storage::*;
use crate::table::table::Table;
use crate::tests::{crud, persistence};

mod table;
mod console;
mod disk_storage;
mod writers;
mod server;
mod tests;

#[cfg(not(test))]
#[tokio::main]
async fn main() {
    let storage = Arc::new(Storage::new());
    Storage::init(storage.clone());

    server::server::Server::new(storage).run();
}

#[test]
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            println!("Starting test");
            let storage = Arc::new(Storage::new());
            println!("Storage created");
            Storage::init(storage.clone());
            println!("Storage initialized");
            crud(storage.clone());
            persistence(storage.clone());
        });


    // let table = table::in_memory::InMemoryTable::new(0, index::HashInMemoryIndex::new(), "test".to_string(), true, 0 ,storage.log_writer.clone());
    // let key = bin_types::BinKey::new(b"key");
    // let value = bin_types::BinValue::new(b"value1");
    // println!("{:?}", value);
    // table.insert(key.clone(), value);
    // let got = table.get(&key);
    // assert_eq!(bin_types::BinValue::new(b"value1"), got.unwrap());
    // let got = table.get(&key);
    // println!("Got: {:?}, needed: {:?}", got.unwrap(), bin_types::BinValue::new(b"value1"));
    //
    // tokio::time::sleep(Duration::from_secs(111111)).await;

    //server::server::Server::new(storage).run();
}