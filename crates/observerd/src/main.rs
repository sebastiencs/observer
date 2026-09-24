mod config;
mod consumer;
mod janitor;
mod query;
mod readiness;

use std::{
    fs,
    path::PathBuf,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use config::Config;
use observer_ingest::{LogsHttpService, LogsIngestService};
use observer_query::QueryEngine;
use observer_wal::{TenantWalRouter, WalWriterConfig};
use readiness::{FilesystemFreeSpace, Readiness};
use tokio::{net::TcpListener, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct AdminState {
    wal: Arc<TenantWalRouter>,
    consumers_failed: Arc<AtomicBool>,
    janitor_failed: Arc<AtomicBool>,
    serving: Arc<AtomicBool>,
    wal_directory: PathBuf,
    data_directory: PathBuf,
    readiness: Arc<Readiness<FilesystemFreeSpace>>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: observerd <config.toml>")?;
    let config = Config::load(path)?;

    fs::create_dir_all(&config.wal_directory)?;
    fs::create_dir_all(&config.data_directory)?;
    let readiness = Arc::new(Readiness::filesystem(config.readiness));
    readiness.ensure_startup(&[&config.wal_directory, &config.data_directory])?;

    let wal = Arc::new(TenantWalRouter::open(
        WalWriterConfig::new(&config.wal_directory),
        config.tokens.tenants(),
    )?);
    let grpc_listener = TcpListener::bind(config.listen.grpc).await?;
    let http_listener = TcpListener::bind(config.listen.http).await?;
    let admin_listener = TcpListener::bind(config.listen.admin).await?;
    let consumers = match consumer::ConsumerSet::start(&config, wal.tenant_ids()) {
        Ok(consumers) => consumers,
        Err(error) => {
            let _ = wal.shutdown().await;
            return Err(error.into());
        }
    };
    let query_listener = match TcpListener::bind(config.listen.query).await {
        Ok(listener) => listener,
        Err(error) => {
            let _ = consumers.shutdown();
            let _ = wal.shutdown().await;
            return Err(error.into());
        }
    };
    let janitor = match janitor::Janitor::start(
        consumers.stores(),
        config.retention.clone(),
        Arc::new(observer_storage::SystemClock),
    ) {
        Ok(janitor) => janitor,
        Err(error) => {
            let _ = consumers.shutdown();
            let _ = wal.shutdown().await;
            return Err(error.into());
        }
    };
    let engine = match QueryEngine::new(config.query.engine.clone()) {
        Ok(engine) => Arc::new(engine),
        Err(error) => {
            janitor.shutdown();
            let _ = consumers.shutdown();
            let _ = wal.shutdown().await;
            return Err(error.into());
        }
    };
    let serving = Arc::new(AtomicBool::new(false));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let grpc = spawn_grpc(
        grpc_listener,
        Arc::clone(&wal),
        config.tokens.clone(),
        shutdown_rx.clone(),
    );
    let http = spawn_http(
        http_listener,
        Arc::clone(&wal),
        config.tokens.clone(),
        shutdown_rx.clone(),
    );
    let query = spawn_query(
        query_listener,
        query::QueryService::new(
            Arc::clone(&engine),
            consumers.stores(),
            config.tokens,
            &config.query,
        ),
        shutdown_rx.clone(),
    );
    let admin = spawn_admin(
        admin_listener,
        AdminState {
            wal: Arc::clone(&wal),
            consumers_failed: consumers.failed_flag(),
            janitor_failed: janitor.failed_flag(),
            serving: Arc::clone(&serving),
            wal_directory: config.wal_directory.clone(),
            data_directory: config.data_directory.clone(),
            readiness,
        },
        shutdown_rx,
    );

    serving.store(true, Ordering::SeqCst);
    wait_for_shutdown().await;
    serving.store(false, Ordering::SeqCst);
    janitor.shutdown();
    let _ = shutdown_tx.send(true);

    // Stop accepting queries and finish in-flight requests before the WAL consumers drain.
    // Dropping the engine afterwards releases its spill directory.
    let query_result = query.await;
    drop(engine);
    let grpc_result = grpc.await;
    let http_result = http.await;
    let admin_result = admin.await;
    let wal_result = wal.shutdown().await;
    let consumer_result = consumers.shutdown();
    query_result??;
    grpc_result??;
    http_result??;
    admin_result??;
    wal_result?;
    consumer_result?;
    Ok(())
}

fn spawn_grpc(
    listener: TcpListener,
    wal: Arc<TenantWalRouter>,
    tokens: observer_ingest::TokenDirectory,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), tonic::transport::Error>> {
    let service = LogsIngestService::new(wal, tokens).into_server(MAX_MESSAGE_SIZE);
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown.wait_for(|stop| *stop).await;
            })
            .await
    })
}

fn spawn_http(
    listener: TcpListener,
    wal: Arc<TenantWalRouter>,
    tokens: observer_ingest::TokenDirectory,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), std::io::Error>> {
    let router = LogsHttpService::new(wal, tokens, MAX_MESSAGE_SIZE).into_router();
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.wait_for(|stop| *stop).await;
            })
            .await
    })
}

fn spawn_query(
    listener: TcpListener,
    service: query::QueryService,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), std::io::Error>> {
    let router = query::router(service);
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.wait_for(|stop| *stop).await;
            })
            .await
    })
}

fn spawn_admin(
    listener: TcpListener,
    state: AdminState,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), std::io::Error>> {
    let router = Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown.wait_for(|stop| *stop).await;
            })
            .await
    })
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<AdminState>) -> StatusCode {
    if state.readiness.is_ready(
        state.serving.load(Ordering::SeqCst),
        state.wal.is_failed()
            || state.consumers_failed.load(Ordering::SeqCst)
            || state.janitor_failed.load(Ordering::SeqCst),
        &[&state.wal_directory, &state.data_directory],
    ) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn wait_for_shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_stays_ok_when_not_ready() {
        assert_eq!(live().await, StatusCode::OK);
    }
}
