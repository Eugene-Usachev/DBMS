#![cfg(test)]
use std::sync::Arc;
use crate::{
    bin_types::{BinKey, BinValue},
    index::HashInMemoryIndex,
    info,
    scheme::scheme::empty_scheme,
    storage::Storage,
    writers::LogWriter
};

#[cfg(test)]
pub fn crud_bench(storage: &'static Storage) {
    const N: usize = 10_400_000;
    const PAR: usize = 256;
    const COUNT: usize = N / PAR;


    let number = Storage::create_in_memory_table(storage, "crud_bench".to_string(), HashInMemoryIndex::new(), false, empty_scheme(), &[]);
    let mut keys = Vec::with_capacity(N);
    let mut values = Vec::with_capacity(N);
    for i in 0..N {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    // necessary for tests, because first test in the Docker always slows down
    {
        let mut joins = Vec::with_capacity(PAR);
        for i in 0..PAR {
            let keys = keys.clone();
            let values = values.clone();
            joins.push(std::thread::spawn(move || unsafe {
                let mut log_writer = LogWriter::new(storage.log_file.clone());
                for j in i * COUNT..(i + 1) * COUNT {
                    (*storage.tables.get())[number].set(keys[j].clone(), values[j].clone(), &mut log_writer);
                }
            }));
        }
        for join in joins {
            join.join().unwrap();
        }

        let mut joins = Vec::with_capacity(PAR);
        for i in 0..PAR {
            let keys = keys.clone();
            joins.push(std::thread::spawn(move || unsafe {
                let mut log_writer = LogWriter::new(storage.log_file.clone());
                for j in i * COUNT..(i + 1) * COUNT {
                    (*storage.tables.get())[number].delete(&keys[j], &mut log_writer);
                }
            }));
        }
        for join in joins {
            join.join().unwrap();
        }
    }

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let keys = keys.clone();
        let values = values.clone();
        joins.push(std::thread::spawn(move || unsafe {
            let mut log_writer = LogWriter::new(storage.log_file.clone());
            for j in i * COUNT..(i + 1) * COUNT {
                (*storage.tables.get())[number].insert(keys[j].clone(), values[j].clone(), &mut log_writer);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    info!("crud_bench: insert took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let storage = storage.clone();
        let keys = keys.clone();
        joins.push(std::thread::spawn(move || unsafe {
            for j in i * COUNT..(i + 1) * COUNT {
                (*storage.tables.get())[number].get(&keys[j]);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    info!("crud_bench: get took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let keys = keys.clone();
        let values = values.clone();
        joins.push(std::thread::spawn(move || unsafe {
            let mut log_writer = LogWriter::new(storage.log_file.clone());
            for j in i * COUNT..(i + 1) * COUNT {
                (*storage.tables.get())[number].set(keys[j].clone(), values[j].clone(), &mut log_writer);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    info!("crud_bench: set took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let storage = storage.clone();
        let keys = keys.clone();
        joins.push(std::thread::spawn(move || unsafe {
            let mut log_writer = LogWriter::new(storage.log_file.clone());
            for j in i * COUNT..(i + 1) * COUNT {
                (*storage.tables.get())[number].delete(&keys[j], &mut log_writer);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    info!("crud_bench: delete took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());
}