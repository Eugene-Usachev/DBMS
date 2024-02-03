use std::sync::atomic::{AtomicUsize};
use std::sync::atomic::Ordering::{SeqCst};
use std::sync::Mutex;

pub struct Node {
    other_machines: Mutex<Box<[String]>>,
    pub version: AtomicUsize,
}

impl Node {
    pub fn new() -> Self {
        Node {
            other_machines: Mutex::new(vec![].into_boxed_slice()),
            version: AtomicUsize::new(0)
        }
    }

    pub fn get_other_machines_addr(&self) -> Box<[String]> {
        self.other_machines.lock().unwrap().clone()
    }

    pub fn append(&self, addr: String) {
        let old_slice = self.other_machines.lock().unwrap();
        let old_len = old_slice.len();
        let new_len = old_len + 1;
        let mut vec = Vec::with_capacity(new_len);
        vec.extend_from_slice(&old_slice);
        vec.push(addr);
        self.version.fetch_add(1, SeqCst);
    }

    pub fn set(&self, values: &[String]) {
        let vec = values.to_vec();
        *self.other_machines.lock().unwrap() = vec.into_boxed_slice();
        self.version.fetch_add(1, SeqCst);
    }
}