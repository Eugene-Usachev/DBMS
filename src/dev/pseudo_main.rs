use std::time::Instant;
use tokio::task;

pub struct M {
    concurrency: usize,
}

impl M {
    #[allow(dead_code)]
    pub(crate) fn new(concurrency: usize) -> Self {
        Self { concurrency }
    }

    pub(crate) async fn bench_async<F>(&self, func: F, n: u32, test_name: &str)
        where
            F: Fn() + Send + 'static + Copy,
    {
        let concurrency = self.concurrency.clone(); // Set the desired concurrency level

        let count = n / concurrency as u32;

        let time_now = Instant::now();

        let mut tasks = Vec::new();

        for _ in 0..concurrency {
            let task = task::spawn(async move {
                for _ in 0..count {
                    func();
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        let elapsed = time_now.elapsed();
        println!("{} elapsed: {:?}", test_name, elapsed);
    }
}