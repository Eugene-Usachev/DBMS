#[cfg(test)]
pub(crate) static TABLES_CREATED: AtomicUsize = AtomicUsize::new(0);

pub mod crud;
pub mod persistence;

use std::sync::atomic::AtomicUsize;
pub use crate::tests::crud::*;
pub use crate::tests::persistence::*;