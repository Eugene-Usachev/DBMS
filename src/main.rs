#![allow(mutable_transmutes)]

#[test]
use std::sync::Arc;
mod index;

mod bin_types;
mod constants;
mod utils;
mod storage;

#[cfg(test)]
use storage::*;
#[cfg(test)]
use crate::tests::{crud, crud_bench, persistence};
use crate::shard::Manager;

mod table;
mod console;
mod disk_storage;
mod writers;
mod server;
mod tests;
mod scheme;
mod connection;
mod node;
mod shard;
mod error;

#[cfg(not(test))]
fn main() {
    let manager = Manager::new();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("Server")
        .build().unwrap().block_on(async move {
            server::server::Server::run(manager.await).await;
        });
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
            crud_bench(storage.clone());
        });
}