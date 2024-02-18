#[cfg(test)]
use std::fs;
use std::sync::Arc;
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
    let storage = Arc::new(Storage::new(["..", constants::paths::PERSISTENCE_DIR].iter().collect()));
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
            info!("Starting test");
            let storage = Arc::new(Storage::new(["test_data"].iter().collect()));
            info!("Storage created");
            Storage::init(storage.clone());
            info!("Storage initialized");
            crud(storage.clone());
            persistence(storage.clone());

            println!();
            crud_bench(storage.clone());

            fs::remove_dir_all("test_data").unwrap();
        });
}