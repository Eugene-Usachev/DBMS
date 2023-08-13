mod server;
mod console;
mod constants;
mod space;
mod settings;
mod utils;

use crate::console::start_message;
use crate::server::server::Server;

fn main() {
    //test_ideal_numbers();

    start_message::start_message();

    Server::new().run();
}


// fn bench_async_map_std(n : usize, keys: &[String]) {
//     let mut maps: Vec<_> = Vec::with_capacity(6);
//     let start = Instant::now();
//     let handles: Vec<_> = (0..6)
//         .map(|table_index| {
//             std::thread::spawn(move || {
//                 let mut space = HashMap::new();
//                 for i in (0..3000000).filter(|i| (i % 6) == table_index) {
//                     space.insert(keys[i].clone(), i);
//                 }
//                 space
//             })
//         })
//         .collect();
//
//     for (i, handle) in handles.into_iter().enumerate() {
//         let space = handle.join().unwrap();
//         maps.push(space);
//     }
//
//     let elapsed = start.elapsed();
//     println!("bench_async_map taken to set {} keys: {:?} len is {} summary: {:?}", n, elapsed, maps.len(), maps[0].len() + maps[1].len() + maps[2].len() + maps[3].len() + maps[4].len() + maps[5].len());
//
//     let start2 = Instant::now();
//     for table_index in 0..6 {
//         for i in (0..3000000).filter(|i| (i % 6) == table_index) {
//             maps[(i % 4) as usize].get(&keys[i].clone());
//         }
//     }
//     let elapsed2 = start2.elapsed();
//     println!("bench_async_map taken to get {} keys: {:?}", n, elapsed2);
// }

// fn bench_amap(n : usize, keys: &[String]) {
//     let mut map = HashMap::new();
//     let start = Instant::now();
//     for i in 0..n {
//         map.insert(keys[i].clone(), i);
//     }
//     let elapsed = start.elapsed();
//     println!("ahash taken to set {} keys: {:?} len is {}", n, elapsed, map.len());
//
//     let start2 = Instant::now();
//     for i in 0..n {
//         if i != *map.get(&keys[i].clone()).unwrap() {
//             println!("ahash failed");
//         }
//     }
//     let elapsed2 = start2.elapsed();
//     println!("ahash taken to get {} keys: {:?}", n, elapsed2);
// }