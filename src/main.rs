#[cfg(test)]
use std::fs;
use std::mem;
mod index;

mod bin_types;
mod constants;
mod utils;
mod storage;

use storage::*;
#[cfg(test)]
use crate::tests::{crud, crud_bench, persistence};

mod table;
mod console;
mod disk_storage;
mod writers;
mod server;
mod tests;
mod scheme;
mod connection;
mod stream;
mod node;

#[cfg(not(test))]
#[tokio::main]
async fn main() {
    let storage = Storage::new(["..", constants::paths::PERSISTENCE_DIR].iter().collect());
    let storage_static = unsafe { mem::transmute::<&Storage, &'static Storage>(&storage) };
    storage_static.init();

    server::server::Server::new(storage_static).run();
}

#[test]
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            info!("Starting test");
            let storage = Storage::new(["test_data"].iter().collect());
            let storage_static = unsafe { mem::transmute::<&Storage, &'static Storage>(&storage) };
            info!("Storage created");
            Storage::init(storage_static);
            info!("Storage initialized");
            crud(storage_static);
            persistence(storage_static);

            println!();
            crud_bench(storage_static);

            fs::remove_dir_all("test_data").unwrap();
        });
}