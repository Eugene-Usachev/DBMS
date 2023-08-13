use std::sync::Arc;
use std::thread;
use std::time::Instant;
use crate::space::space::{Space, SpaceEngineType, SpaceInterface};

pub fn test_ideal_numbers() {
    let mut total_count = 3_000_000;
    let mut parallel_threads = 24; // NUM_CPUS * 4
    let mut count_of_parts = 256; // NUM_CPUS * 42
    let times = 5;

    let mut insert_time = 0f64;
    let mut get_time = 0f64;
    let mut capacity = 0f64;
    let mut length = 0f64;
    for _ in 0..times {
        let res = test(total_count.clone(), parallel_threads, count_of_parts as u64);
        insert_time += res.insert_speed;
        get_time += res.get_speed;
        capacity += res.capacity as f64;
        length += res.length as f64;
    }
    let mut insert_time_average = insert_time / times as f64;
    let mut get_time_average = get_time / times as f64;
    let mut capacity_average = capacity / times as f64;
    let mut length_average = length / times as f64;
    let usable_memory = length_average / capacity_average;
    println!("insert_time_average: {}, get_time_average: {}, usable_memory: {}", insert_time_average, get_time_average, usable_memory);
}

struct TestResult {
    pub insert_speed: f64,
    pub get_speed: f64,
    pub length: usize,
    pub capacity: usize
}

fn test(total_count: u64, parallel_threads: u64, count_of_parts: u64) -> TestResult {
    let space: Arc<Space> = Arc::new(Space::new(SpaceEngineType::InMemory, count_of_parts.clone() as usize));
    let mut joins = Vec::new();

    let time_insert = Instant::now();
    let count = total_count / parallel_threads;

    for thread in 0..parallel_threads {
        let mut space = Arc::clone(&space);
        let count = count.clone();
        joins.push(thread::spawn(move || {
            for i in thread * count..thread * count + count {
                space.set(i as i32, b"value1".to_vec(), i as usize);
            }
        }))
    }

    for join in joins {
        join.join().unwrap();
    }

    let elapsed_insert = time_insert.elapsed();

    let mut joins = Vec::new();

    let time_get = Instant::now();

    for thread in 0..parallel_threads {
        let mut space = Arc::clone(&space);
        let count = count.clone();
        joins.push(thread::spawn(move || {
            for i in thread * count..thread * count + count {
                space.get(i as i32, i as usize);
            }
        }))
    }

    let binding = Arc::clone(&space);
    let space_read = binding.data[0].read().unwrap();

    let length = space_read.len();
    let capacity = space_read.capacity();

    for join in joins {
        join.join().unwrap();
    }

    let elapsed_get = time_get.elapsed();

    TestResult {
        get_speed: total_count as f64 / elapsed_get.as_secs_f64(),
        insert_speed: total_count as f64 / elapsed_insert.as_secs_f64(),
        length: length * count_of_parts as usize,
        capacity: capacity * count_of_parts as usize
    }
}