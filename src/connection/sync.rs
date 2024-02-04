use tokio::io::BufWriter;
use tokio::net::tcp::{OwnedWriteHalf};
use crate::connection::{BufConnection, BufReader, Status};

#[derive(Clone)]
pub struct SyncBufConnection {
    ptr: *mut BufConnection,
}

impl<'a> SyncBufConnection {
    pub fn new(mut connection: BufConnection) -> Self {
        let ptr = Box::into_raw(Box::new(connection));
        SyncBufConnection {
            ptr
        }
    }

    #[inline(always)]
    pub fn reader(&self) -> &mut BufReader {
        unsafe { (&mut *self.ptr).reader() }
    }

    #[inline(always)]
    pub async fn read_exact(&self, buf: &mut [u8]) -> Status {
        self.get().read_exact(buf).await
    }

    #[inline(always)]
    pub fn get(&self) -> &mut BufConnection {
        unsafe { &mut *self.ptr}
    }

    #[inline(always)]
    pub async fn read_request(&mut self) -> (Status, usize) {
        self.get().read_request().await
    }

    #[inline(always)]
    pub fn writer(&self) -> &mut BufWriter<OwnedWriteHalf> {
        unsafe { (&mut *self.ptr).writer() }
    }

    #[inline(always)]
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.get().flush().await
    }

    #[inline(always)]
    pub async fn read_message(&self) -> (&[u8], Status) {
        self.get().read_message().await
    }

    #[inline(always)]
    pub fn close(&mut self) -> std::io::Result<()> {
        self.get().close()
    }
}

impl Drop for SyncBufConnection {
    fn drop(&mut self) {
        unsafe {
            Box::from_raw(self.ptr);
        }
    }
}

unsafe impl<'a> Send for SyncBufConnection {}
unsafe impl<'a> Sync for SyncBufConnection {}