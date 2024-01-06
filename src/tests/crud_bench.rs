use std::intrinsics::black_box;
use std::sync::Arc;
use crate::bin_types::{BinKey, BinValue};
use crate::index::HashInMemoryIndex;
use crate::storage::Storage;

// #[cfg(test)]
pub fn crud_bench(storage: Arc<Storage>) {
    const N: usize = 10_400_000;
    const PAR: usize = 256;
    const COUNT: usize = N / PAR;

    let number = Storage::create_in_memory_table(storage.clone(), "crud_bench".to_string(), HashInMemoryIndex::new(), false);
    let mut keys = Vec::with_capacity(N);
    let mut values = Vec::with_capacity(N);
    for i in 0..N {
        keys.push(BinKey::new(format!("key{i}").as_bytes()));
        values.push(BinValue::new(format!("value{i}").as_bytes()));
    }
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let storage = storage.clone();
        let keys = keys.clone();
        let values = values.clone();
        joins.push(std::thread::spawn(move || {
            let tables = &mut storage.tables.read().unwrap();
            for j in i * COUNT..(i + 1) * COUNT {
                tables[number].insert(keys[j].clone(), values[j].clone(), &mut [], &mut 0);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    println!("crud_bench: insert took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let storage = storage.clone();
        let keys = keys.clone();
        joins.push(std::thread::spawn(move || {
            let tables = &mut storage.tables.read().unwrap();
            for j in i * COUNT..(i + 1) * COUNT {
                black_box(tables[number].get(&keys[j]));
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    println!("crud_bench: get took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let storage = storage.clone();
        let keys = keys.clone();
        let values = values.clone();
        joins.push(std::thread::spawn(move || {
            let tables = &mut storage.tables.read().unwrap();
            for j in i * COUNT..(i + 1) * COUNT {
                tables[number].set(keys[j].clone(), values[j].clone(), &mut [], &mut 0);
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    println!("crud_bench: set took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());

    let mut joins = Vec::with_capacity(PAR);
    let start = std::time::Instant::now();
    for i in 0..PAR {
        let storage = storage.clone();
        let keys = keys.clone();
        joins.push(std::thread::spawn(move || {
            let tables = &mut storage.tables.read().unwrap();
            for j in i * COUNT..(i + 1) * COUNT {
                black_box(tables[number].delete(&keys[j], &mut [], &mut 0));
            }
        }));
    }
    for join in joins {
        join.join().unwrap();
    }
    println!("crud_bench: delete took {} ms and has {} RPS", start.elapsed().as_millis(), N as f64 / start.elapsed().as_secs_f64());
}