#![allow(internal_features)]
#![feature(core_intrinsics)]
use std::sync::Arc;
mod index;

mod bin_types;
mod constants;
mod utils;
mod storage;

use storage::*;
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
}