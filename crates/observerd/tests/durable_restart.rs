#![cfg(unix)]

mod support;

use std::time::Duration;

use observer_protocol::otlp::ExportLogsServiceRequest;
use prost::Message;
use support::{
    ListenAddrs, Observerd, TENANT, TEST_TIMEOUT, frames_on_disk, logs_request, post_logs,
    reserve_ports, write_config,
};
use tokio::time::timeout;

const START_ATTEMPTS: usize = 3;

#[tokio::test]
async fn acknowledged_http_logs_survive_sigterm_and_restart() {
    timeout(TEST_TIMEOUT * 3, run_restart_scenario())
        .await
        .expect("e2e scenario timed out");
}

async fn run_restart_scenario() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let first = logs_request("before-restart");
    let second = logs_request("after-restart");

    let (listen, mut daemon) = start_with_retries(&config_path, &wal_directory).await;
    post_logs(listen.http, &first).await;
    daemon.terminate().await;

    let mut daemon = restart(&config_path, listen).await;
    post_logs(listen.http, &second).await;
    daemon.terminate().await;

    let frames = frames_on_disk(&wal_directory);
    assert_eq!(
        frames.len(),
        2,
        "expected two durable frames, found {}",
        frames.len()
    );
    assert_eq!(frames[0].sequence, 0);
    assert_eq!(frames[1].sequence, 1);
    assert_eq!(frames[0].tenant_id, TENANT);
    assert_eq!(frames[1].tenant_id, TENANT);
    assert_eq!(
        ExportLogsServiceRequest::decode(frames[0].payload.clone()).expect("first payload"),
        first
    );
    assert_eq!(
        ExportLogsServiceRequest::decode(frames[1].payload.clone()).expect("second payload"),
        second
    );
}

async fn start_with_retries(
    config_path: &std::path::Path,
    wal_directory: &std::path::Path,
) -> (ListenAddrs, Observerd) {
    let mut last_error = String::from("no attempts");
    for _ in 0..START_ATTEMPTS {
        let listen = reserve_ports();
        write_config(config_path, wal_directory, listen);
        let mut daemon = Observerd::spawn(config_path);
        match timeout(TEST_TIMEOUT, daemon.wait_ready(listen.admin)).await {
            Ok(Ok(())) => return (listen, daemon),
            Ok(Err(error)) => last_error = error,
            Err(_) => last_error = "timed out waiting for first /ready".to_owned(),
        }
    }
    panic!("failed to start observerd after {START_ATTEMPTS} attempts: {last_error}");
}

async fn restart(config_path: &std::path::Path, listen: ListenAddrs) -> Observerd {
    let mut last_error = String::from("no attempts");
    for _ in 0..START_ATTEMPTS {
        let mut daemon = Observerd::spawn(config_path);
        match timeout(TEST_TIMEOUT, daemon.wait_ready(listen.admin)).await {
            Ok(Ok(())) => return daemon,
            Ok(Err(error)) => last_error = error,
            Err(_) => last_error = "timed out waiting for restart /ready".to_owned(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("failed to restart observerd after {START_ATTEMPTS} attempts: {last_error}");
}
