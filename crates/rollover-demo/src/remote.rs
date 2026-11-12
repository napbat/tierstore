//! In-process stand-in for a remote store (database, service): adds
//! artificial latency to every operation and counts fetches, so the demo
//! can prove which reads actually crossed the "network".

use std::collections::HashMap;
use std::future::ready;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tierstore::{Displaced, Tier, TierRead, TierWrite};

#[derive(Debug)]
pub struct RemoteStore {
    data: Mutex<HashMap<String, Vec<u8>>>,
    fetches: AtomicUsize,
    latency: Duration,
}

impl RemoteStore {
    pub fn new(latency: Duration) -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            fetches: AtomicUsize::new(0),
            latency,
        }
    }

    /// Number of `get`s that reached the remote.
    pub fn fetches(&self) -> usize {
        self.fetches.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<u8>>> {
        self.data.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Simulated network round-trip. Blocking a thread is fine here: the
    /// demo drives everything on one thread with a spin executor.
    fn round_trip(&self) {
        std::thread::sleep(self.latency);
    }
}

impl Tier for RemoteStore {
    type Key = String;
    type Value = Vec<u8>;
    type Error = io::Error;

    fn name(&self) -> &'static str {
        "remote"
    }
}

// The simulated round trip blocks, so each future is ready when it is created.
impl TierRead for RemoteStore {
    fn get(&self, key: &String) -> impl Future<Output = io::Result<Option<Vec<u8>>>> {
        self.round_trip();
        self.fetches.fetch_add(1, Ordering::Relaxed);
        ready(Ok(self.lock().get(key).cloned()))
    }

    fn exists(&self, key: &String) -> impl Future<Output = io::Result<bool>> {
        self.round_trip();
        ready(Ok(self.lock().contains_key(key)))
    }
}

impl TierWrite for RemoteStore {
    fn put(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> impl Future<Output = io::Result<Displaced<String, Vec<u8>>>> {
        self.round_trip();
        self.lock().insert(key, value);
        ready(Ok(Displaced::new()))
    }

    fn delete(&self, key: &String) -> impl Future<Output = io::Result<bool>> {
        self.round_trip();
        ready(Ok(self.lock().remove(key).is_some()))
    }
}
