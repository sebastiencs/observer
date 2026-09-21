mod config;

use std::{
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use config::Config;
use observer_ingest::{LogsHttpService, LogsIngestService};
use observer_wal::{TenantWalRouter, WalWriterConfig};
use tokio::{net::TcpListener, sync::watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct AdminState {
    wal: Arc<TenantWalRouter>,
    serving: Arc<AtomicBool>,
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

    let wal = Arc::new(TenantWalRouter::open(
        WalWriterConfig::new(&config.wal_directory),
        config.tokens.tenants(),
    )?);
    let serving = Arc::new(AtomicBool::new(false));

    let grpc_listener = TcpListener::bind(config.listen.grpc).await?;
    let http_listener = TcpListener::bind(config.listen.http).await?;
    let admin_listener = TcpListener::bind(config.listen.admin).await?;

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
        config.tokens,
        shutdown_rx.clone(),
    );
    let admin = spawn_admin(
        admin_listener,
        AdminState {
            wal: Arc::clone(&wal),
            serving: Arc::clone(&serving),
        },
        shutdown_rx,
    );

    serving.store(true, Ordering::SeqCst);
    wait_for_shutdown().await;
    serving.store(false, Ordering::SeqCst);
    let _ = shutdown_tx.send(true);

    grpc.await??;
    http.await??;
    admin.await??;
    wal.shutdown().await?;
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
    if state.serving.load(Ordering::SeqCst) && !state.wal.is_failed() {
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
