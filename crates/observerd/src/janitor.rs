//! Startup and periodic receive-time retention.
//!
//! One pass runs before the daemon is ready. Later passes run on a background thread. A failed
//! pass marks the janitor unhealthy until a later pass completes.

use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use observer_storage::{Clock, CollectOptions, RetireOptions, Store};

use crate::config::RetentionConfig;

/// Files hidden and files unlinked by one complete pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JanitorReport {
    /// Tenant-relative paths hidden during the pass.
    pub retired: Vec<String>,
    /// Rows in the files hidden during the pass.
    pub retired_rows: u64,
    /// Bytes recorded for the files hidden during the pass.
    pub retired_bytes: u64,
    /// Tenant-relative paths unlinked during the pass.
    pub collected: Vec<String>,
}

/// Why a retention pass could not finish.
#[derive(Debug)]
pub enum JanitorError {
    /// `tenant` could not be retired or collected.
    Store { tenant: String, detail: String },
    /// A retention duration does not fit in nanoseconds.
    Duration,
}

impl std::fmt::Display for JanitorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store { tenant, detail } => {
                write!(formatter, "janitor failed for tenant {tenant}: {detail}")
            }
            Self::Duration => formatter.write_str("retention duration is too large"),
        }
    }
}

impl std::error::Error for JanitorError {}

struct Control {
    state: Mutex<ControlState>,
    wake: Condvar,
}

struct ControlState {
    stop: bool,
    run: bool,
    hold: bool,
    entered: bool,
    finished: bool,
}

impl Control {
    fn new() -> Self {
        Self {
            state: Mutex::new(ControlState {
                stop: false,
                run: false,
                hold: false,
                entered: false,
                finished: false,
            }),
            wake: Condvar::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ControlState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// `true` when the thread should exit instead of running a pass.
    fn wait(&self, delay: Duration) -> bool {
        let mut state = self.lock();
        let deadline = Instant::now() + delay;
        loop {
            if state.stop {
                return true;
            }
            if state.run {
                state.run = false;
                return false;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, result) = self
                .wake
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(|error| error.into_inner());
            state = guard;
            if result.timed_out() {
                return state.stop;
            }
        }
    }

    /// `true` when shutdown was requested while this pass is in progress.
    fn begin(&self) -> bool {
        let mut state = self.lock();
        state.entered = true;
        self.wake.notify_all();
        while state.hold && !state.stop {
            state = self
                .wake
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.stop
    }

    fn note_finished(&self) {
        let mut state = self.lock();
        state.finished = true;
        self.wake.notify_all();
    }

    #[cfg(test)]
    fn request(&self) {
        let mut state = self.lock();
        state.run = true;
        state.finished = false;
        state.entered = false;
        self.wake.notify_all();
    }

    #[cfg(test)]
    fn wait_finished(&self) {
        let mut state = self.lock();
        while !state.finished && !state.stop {
            state = self
                .wake
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    fn stop(&self) {
        let mut state = self.lock();
        state.stop = true;
        state.hold = false;
        self.wake.notify_all();
    }
}

/// Background retention loop. The startup pass has already completed when this exists.
pub struct Janitor {
    control: Arc<Control>,
    failed: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl Janitor {
    /// Run one pass now, then repeat it on a jittered interval until [`Janitor::shutdown`].
    ///
    /// # Errors
    ///
    /// Returns [`JanitorError`] when the startup pass cannot finish. No thread is left running.
    pub fn start(
        stores: HashMap<String, Arc<Store>>,
        retention: RetentionConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, JanitorError> {
        let control = Arc::new(Control::new());
        run_pass(&stores, &retention, clock.as_ref(), &control)?;
        let failed = Arc::new(AtomicBool::new(false));
        let thread_control = Arc::clone(&control);
        let thread_failed = Arc::clone(&failed);
        let join = thread::Builder::new()
            .name("observer-janitor".to_owned())
            .spawn(move || {
                loop {
                    let delay = jittered_delay(retention.janitor_interval, clock.unix_nanos());
                    if thread_control.wait(delay) {
                        break;
                    }
                    match run_pass(&stores, &retention, clock.as_ref(), &thread_control) {
                        Ok(_) => thread_failed.store(false, Ordering::SeqCst),
                        Err(JanitorError::Store { .. } | JanitorError::Duration) => {
                            thread_failed.store(true, Ordering::SeqCst);
                        }
                    }
                    thread_control.note_finished();
                }
            })
            .expect("spawn janitor");
        Ok(Self {
            control,
            failed,
            join: Some(join),
        })
    }

    /// Shared flag set while the latest periodic pass failed.
    #[must_use]
    pub fn failed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.failed)
    }

    /// Stop the background thread and wait for the current pass to return.
    pub fn shutdown(mut self) {
        self.control.stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for Janitor {
    fn drop(&mut self) {
        self.control.stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn run_pass(
    stores: &HashMap<String, Arc<Store>>,
    retention: &RetentionConfig,
    clock: &dyn Clock,
    control: &Control,
) -> Result<JanitorReport, JanitorError> {
    let mut tenants: Vec<_> = stores.keys().cloned().collect();
    tenants.sort();
    let now = clock.unix_nanos();
    let mut report = JanitorReport::default();
    for tenant in tenants {
        if control.begin() {
            return Ok(report);
        }
        let store = &stores[&tenant];
        let policy = retention
            .tenants
            .get(&tenant)
            .copied()
            .unwrap_or(retention.default);
        if let Some(duration) = policy {
            let cutoff = now.saturating_sub(duration_nanos(duration)?);
            let retired = store
                .retire_before(cutoff, &RetireOptions::default())
                .map_err(|error| JanitorError::Store {
                    tenant: tenant.clone(),
                    detail: error.to_string(),
                })?;
            report.retired.extend(retired.paths);
            report.retired_rows = report.retired_rows.saturating_add(retired.rows);
            report.retired_bytes = report.retired_bytes.saturating_add(retired.bytes);
        }
        let collected = store
            .collect_retired(&CollectOptions::default())
            .map_err(|error| JanitorError::Store {
                tenant,
                detail: error.to_string(),
            })?;
        report.collected.extend(collected.removed);
    }
    Ok(report)
}

fn duration_nanos(duration: Duration) -> Result<u64, JanitorError> {
    u64::try_from(duration.as_nanos()).map_err(|_| JanitorError::Duration)
}

/// Wait between three quarters and all of `interval`, using `now_nanos` as the jitter source.
fn jittered_delay(interval: Duration, now_nanos: u64) -> Duration {
    let millis = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
    let span = millis / 4;
    let jitter = if span == 0 { 0 } else { now_nanos % span };
    Duration::from_millis(millis.saturating_sub(span).saturating_add(jitter))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::thread;
    use std::time::Duration;

    use super::{Control, Janitor, jittered_delay, run_pass};
    use crate::config::RetentionConfig;
    use bytes::Bytes;
    use observer_protocol::otlp::{
        AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
    };
    use observer_storage::{
        Clock, DynamicLimits, ManualClock, MemtableConfig, PublishOptions, Store, decode_logs_frame,
    };
    use observer_wal::{Frame, FrameSignal};
    use prost::Message;

    struct SharedClock(AtomicU64);

    impl Clock for SharedClock {
        fn unix_nanos(&self) -> u64 {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    fn retention(
        default: Option<Duration>,
        tenants: Vec<(String, Option<Duration>)>,
    ) -> RetentionConfig {
        RetentionConfig {
            default,
            janitor_interval: Duration::from_secs(3600),
            tenants: tenants.into_iter().collect(),
        }
    }

    fn publish(directory: &std::path::Path, received: u64) -> Store {
        let store = Store::open(
            directory,
            "tenant-a",
            MemtableConfig {
                max_rows: 100,
                max_bytes: u64::MAX,
                max_age: Duration::from_secs(60),
                max_frozen: 4,
                max_dynamic_columns: 32,
            },
            std::sync::Arc::new(ManualClock::new(0)),
        )
        .expect("open");
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("row".to_owned())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let logs = decode_logs_frame(
            &Frame {
                sequence: 0,
                signal: FrameSignal::Logs,
                received_at_unix_nanos: received,
                tenant_id: "tenant-a".to_owned(),
                payload: Bytes::from(request.encode_to_vec()),
            },
            DynamicLimits {
                max_depth: 4,
                max_columns: 32,
            },
        )
        .expect("decode");
        store.append(&logs).expect("append");
        store.rotate().expect("rotate");
        store
            .publish(&PublishOptions::default())
            .expect("publish")
            .expect("commit");
        store
    }

    #[test]
    fn jitter_stays_inside_the_last_quarter_of_the_interval() {
        let interval = Duration::from_millis(1000);
        let low = jittered_delay(interval, 0);
        let high = jittered_delay(interval, u64::MAX);
        assert!(low >= Duration::from_millis(750));
        assert!(low <= interval);
        assert!(high >= Duration::from_millis(750));
        assert!(high <= interval);
    }

    #[test]
    fn policy_uses_the_tenant_override_and_forever_skips_retirement() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = std::sync::Arc::new(publish(directory.path(), 1_000));
        let clock = SharedClock(AtomicU64::new(10_000));
        let stores = HashMap::from([("tenant-a".to_owned(), std::sync::Arc::clone(&store))]);
        let forever = run_pass(
            &stores,
            &retention(None, vec![("tenant-a".to_owned(), None)]),
            &clock,
            &Control::new(),
        )
        .expect("forever");
        assert!(forever.retired.is_empty());
        assert!(forever.collected.is_empty());

        let kept = run_pass(
            &stores,
            &retention(
                Some(Duration::from_nanos(1)),
                vec![("tenant-a".to_owned(), Some(Duration::from_secs(10_000)))],
            ),
            &clock,
            &Control::new(),
        )
        .expect("override");
        assert!(kept.retired.is_empty());
        let finite = run_pass(
            &stores,
            &retention(Some(Duration::from_nanos(1)), Vec::new()),
            &clock,
            &Control::new(),
        )
        .expect("default");
        assert_eq!(finite.retired.len(), 1);
        assert_eq!(finite.retired_rows, 1);
        assert_eq!(finite.collected.len(), 1);
        let again = run_pass(
            &stores,
            &retention(Some(Duration::from_nanos(1)), Vec::new()),
            &clock,
            &Control::new(),
        )
        .expect("repeat");
        assert!(again.retired.is_empty());
        assert!(again.collected.is_empty());
    }

    #[test]
    fn a_failed_pass_recovers_on_the_next_attempt() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = std::sync::Arc::new(publish(directory.path(), 1_000));
        let clock = SharedClock(AtomicU64::new(10_000));
        let stores = HashMap::from([("tenant-a".to_owned(), store)]);
        let policy = retention(Some(Duration::from_nanos(1)), Vec::new());
        let tenant_dir = directory.path().join("tenants/tenant-a/retirements");
        std::fs::create_dir_all(directory.path().join("tenants/tenant-a")).expect("tenant");
        std::fs::write(&tenant_dir, b"not a directory").expect("block");
        let error = run_pass(&stores, &policy, &clock, &Control::new()).expect_err("blocked");
        assert!(matches!(error, super::JanitorError::Store { .. }));
        assert!(!error.to_string().contains("secret"));
        std::fs::remove_file(&tenant_dir).expect("unblock");
        let report = run_pass(&stores, &policy, &clock, &Control::new()).expect("retry");
        assert_eq!(report.retired.len(), 1);
    }

    #[test]
    fn shutdown_stops_an_idle_wait_and_an_active_pass() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = std::sync::Arc::new(publish(directory.path(), 1_000));
        let stores = HashMap::from([("tenant-a".to_owned(), std::sync::Arc::clone(&store))]);
        let clock = std::sync::Arc::new(SharedClock(AtomicU64::new(0)));
        let idle = Janitor::start(
            stores,
            retention(Some(Duration::from_secs(3600)), Vec::new()),
            clock,
        )
        .expect("start");
        idle.shutdown();

        let directory = tempfile::tempdir().expect("tempdir");
        let store = std::sync::Arc::new(publish(directory.path(), 1_000));
        let stores = HashMap::from([("tenant-a".to_owned(), store)]);
        let control = std::sync::Arc::new(Control::new());
        {
            let mut state = control.lock();
            state.hold = true;
        }
        let retention = retention(Some(Duration::from_nanos(1)), Vec::new());
        let clock = std::sync::Arc::new(SharedClock(AtomicU64::new(10_000)));
        let thread_control = std::sync::Arc::clone(&control);
        let join = thread::spawn(move || {
            thread_control.wait(Duration::from_secs(3600));
            let _ = run_pass(&stores, &retention, clock.as_ref(), &thread_control);
        });
        {
            let mut state = control.lock();
            state.run = true;
            control.wake.notify_all();
            while !state.entered {
                state = control
                    .wake
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
            }
        }
        control.stop();
        join.join().expect("join");
    }

    #[test]
    fn a_periodic_failure_clears_after_the_next_successful_pass() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = std::sync::Arc::new(publish(directory.path(), 1_000));
        let clock = std::sync::Arc::new(SharedClock(AtomicU64::new(0)));
        let stores = HashMap::from([("tenant-a".to_owned(), store)]);
        let janitor = Janitor::start(
            stores,
            retention(Some(Duration::from_nanos(1)), Vec::new()),
            std::sync::Arc::clone(&clock) as std::sync::Arc<dyn Clock>,
        )
        .expect("start");
        assert!(!janitor.failed.load(std::sync::atomic::Ordering::SeqCst));
        clock.0.store(10_000, std::sync::atomic::Ordering::Relaxed);
        let tenant_dir = directory.path().join("tenants/tenant-a/retirements");
        std::fs::write(&tenant_dir, b"not a directory").expect("block");
        janitor.control.request();
        janitor.control.wait_finished();
        assert!(janitor.failed.load(std::sync::atomic::Ordering::SeqCst));
        std::fs::remove_file(&tenant_dir).expect("unblock");
        janitor.control.request();
        janitor.control.wait_finished();
        assert!(!janitor.failed.load(std::sync::atomic::Ordering::SeqCst));
        janitor.shutdown();
    }
}
