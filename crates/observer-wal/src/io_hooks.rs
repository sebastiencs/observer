use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tokio::sync::Notify;

use crate::WalError;

/// Test-only controls for WAL `sync_data` and writer admission.
#[derive(Debug)]
pub struct WalIoHooks {
    sync_count: AtomicUsize,
    fail_next_sync: AtomicBool,
    hold_next_sync: AtomicBool,
    sync_waiting: AtomicBool,
    sync_started: Notify,
    release_sync: (Mutex<bool>, Condvar),
    hold_admission: AtomicBool,
    admission_waiting: AtomicBool,
    admission_started: Notify,
    admission_release: Notify,
}

impl Default for WalIoHooks {
    fn default() -> Self {
        Self::new()
    }
}

impl WalIoHooks {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sync_count: AtomicUsize::new(0),
            fail_next_sync: AtomicBool::new(false),
            hold_next_sync: AtomicBool::new(false),
            sync_waiting: AtomicBool::new(false),
            sync_started: Notify::new(),
            release_sync: (Mutex::new(false), Condvar::new()),
            hold_admission: AtomicBool::new(false),
            admission_waiting: AtomicBool::new(false),
            admission_started: Notify::new(),
            admission_release: Notify::new(),
        }
    }

    #[must_use]
    pub fn sync_count(&self) -> usize {
        self.sync_count.load(Ordering::SeqCst)
    }

    pub fn fail_next_sync(&self) {
        self.fail_next_sync.store(true, Ordering::SeqCst);
    }

    pub fn hold_next_sync(&self) {
        *self
            .release_sync
            .0
            .lock()
            .expect("sync release lock poisoned") = false;
        self.hold_next_sync.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_sync_waiting(&self) -> bool {
        self.sync_waiting.load(Ordering::SeqCst)
    }

    pub fn sync_started(&self) -> &Notify {
        &self.sync_started
    }

    pub fn release_sync(&self) {
        let mut released = self
            .release_sync
            .0
            .lock()
            .expect("sync release lock poisoned");
        *released = true;
        self.release_sync.1.notify_all();
    }

    pub fn hold_admission(&self) {
        self.hold_admission.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_admission_waiting(&self) -> bool {
        self.admission_waiting.load(Ordering::SeqCst)
    }

    pub fn admission_started(&self) -> &Notify {
        &self.admission_started
    }

    pub fn release_admission(&self) {
        self.admission_release.notify_one();
    }

    pub(crate) fn on_sync(&self) -> Result<(), WalError> {
        self.sync_count.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            return Err(WalError::Io(std::io::Error::other("injected sync error")));
        }
        if self.hold_next_sync.swap(false, Ordering::SeqCst) {
            self.sync_waiting.store(true, Ordering::SeqCst);
            self.sync_started.notify_one();
            let mut released = self
                .release_sync
                .0
                .lock()
                .expect("sync release lock poisoned");
            while !*released {
                released = self
                    .release_sync
                    .1
                    .wait(released)
                    .expect("sync release condvar poisoned");
            }
            self.sync_waiting.store(false, Ordering::SeqCst);
        }
        Ok(())
    }

    pub(crate) async fn wait_admission_if_held(&self) {
        if self.hold_admission.swap(false, Ordering::SeqCst) {
            self.admission_waiting.store(true, Ordering::SeqCst);
            self.admission_started.notify_one();
            self.admission_release.notified().await;
            self.admission_waiting.store(false, Ordering::SeqCst);
        }
    }
}
