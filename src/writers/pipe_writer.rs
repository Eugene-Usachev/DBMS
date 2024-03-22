#[allow(unused)]
// TODO: maybe remove? And doesn't work, because cvar sets to true in flush() and doesn't reset. If fix, we have no performance benefits

use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant}
};

#[allow(dead_code)]
pub struct PipeWriter {
    pub file: Arc<Mutex<BufWriter<File>>>,
    pub last_write: Arc<Mutex<Instant>>,
    pub cvar: Arc<(Mutex<bool>, Condvar)>
}

#[allow(dead_code)]
impl PipeWriter {
    pub fn new(path: String) -> Self {
        let file = File::create(path).unwrap();
        let s = Self {
            file: Arc::new(Mutex::new(BufWriter::with_capacity(16*1024, file))),
            last_write: Arc::new(Mutex::new(Instant::now())),
            cvar: Arc::new((Mutex::new(false), Condvar::new())),
        };

        return s;
    }

    #[inline(always)]
    pub fn write(&self, value: &Vec<u8>) {
        let mut file = self.file.lock().unwrap();
        if file.capacity() - file.buffer().len() < value.len() {
            Self::flush_locked(&mut file, self.cvar.clone());
        }
        file.write_all(&value).unwrap();
        drop(file);
        *self.last_write.lock().unwrap() = Instant::now();

        let (lock, cvar) = &*self.cvar;
        let mut started = lock.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }
    }

    pub fn flush_locked(file: &mut MutexGuard<BufWriter<File>>, cvar: Arc<(Mutex<bool>, Condvar)>) {
        file.flush().unwrap();
        let (lock, cvar) = &*cvar;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }

    pub fn change_file_with_flush(&self, file: File) {
        let mut lock = self.file.lock().unwrap();
        Self::flush_locked(&mut lock, self.cvar.clone());
        *lock = BufWriter::with_capacity(800*1024*1024, file);
    }

    pub fn flush(file: Arc<Mutex<BufWriter<File>>>, cvar: Arc<(Mutex<bool>, Condvar)>) {
        file.lock().unwrap().flush().unwrap();
        let (lock, cvar) = &*cvar;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }

    pub fn start_flush_thread(&self) {
        let file = Arc::clone(&self.file);
        let last_write = Arc::clone(&self.last_write);
        let cvar = Arc::clone(&self.cvar);
        let time_to_sleep = Duration::from_micros(100);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(time_to_sleep).await;
                let duration_since_last_write = last_write.lock().unwrap().elapsed();
                if duration_since_last_write >= time_to_sleep {
                    Self::flush(file.clone(), cvar.clone());
                }
            }
        });
    }
}