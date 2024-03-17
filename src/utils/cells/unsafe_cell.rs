use std::cell::UnsafeCell as StdUnsafeCell;

/// UnsafeCell uses [`std::cell::UnsafeCell`] and unsafe code! It was created to simplify the use of [`std::cell::UnsafeCell`].
///
/// For debugging you need to take in mind that the [`UnsafeCell::get`] and [`UnsafeCell::get_mut`] methods are unsafe under the hood.
pub struct UnsafeCell<T> {
    inner: StdUnsafeCell<T>
}

impl<T> UnsafeCell<T> {
    pub fn new(t: T) -> UnsafeCell<T> {
        UnsafeCell { inner: StdUnsafeCell::new(t) }
    }

    pub fn get(&self) -> &T {
        unsafe { &*self.inner.get() }
    }

    pub fn get_mut(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

unsafe impl<T: Sync> Sync for UnsafeCell<T> {}
unsafe impl<T: Send> Send for UnsafeCell<T> {}