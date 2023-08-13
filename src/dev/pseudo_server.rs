use crate::Way;

pub struct Server {
    pub concurrency: usize,
    pub ways: Vec<Way>,
}

impl Server {
    #[allow(dead_code)]
    pub fn new(concurrency: usize) -> Self {
        Self { concurrency, ways: Vec::new() }
    }

    #[allow(dead_code)]
    pub(crate) fn start(&mut self) {
        for _ in 0..self.concurrency {
            self.ways.push(Way::new());
        }
    }
}