use std::time::{Instant};

#[allow(dead_code)]
pub fn bench<F>(func: F, n: u32, test_name: &str)
    where
        F: Fn(),
{
    let time_now = Instant::now();

    for _ in 0..n {
        func();
    }

    let elapsed = time_now.elapsed();
    println!("{} elapsed: {:?}", test_name, elapsed);
}
