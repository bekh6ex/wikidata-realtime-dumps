use actix_rt::Arbiter;
use core::num::NonZeroUsize;
use std::iter::Cycle;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

#[derive(Clone, Debug)]
pub struct ArbiterPool {
    pool: Arc<Mutex<Cycle<IntoIter<Arc<Arbiter>>>>>,
}

impl ArbiterPool {
    pub fn new(n: NonZeroUsize) -> Self {
        let vec = (0..n.get())
            .map(|_| Arc::new(Arbiter::new()))
            .collect::<Vec<_>>();
        let cycle: Cycle<IntoIter<Arc<Arbiter>>> = vec.into_iter().cycle();

        let pool = Arc::new(Mutex::new(cycle));
        ArbiterPool { pool }
    }

    pub fn next(&self) -> impl AsRef<Arbiter> {
        let mut pool = self.pool.lock().unwrap();
        pool.next().unwrap()
    }
}
