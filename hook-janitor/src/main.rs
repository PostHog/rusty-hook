use axum::Router;
use cleanup::Cleaner;
use config::Config;
use envconfig::Envconfig;
use eyre::Result;
use futures::future::{select, Either};
use std::time::Duration;
use tokio::sync::Semaphore;

use hook_common::metrics;

mod cleanup;
mod config;
mod handlers;

async fn listen(app: Router, bind: String) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn cleanup_loop(cleaner: Cleaner, interval_secs: u64) -> Result<()> {
    let semaphore = Semaphore::new(1);
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        let _permit = semaphore.acquire().await;
        interval.tick().await;
        cleaner.cleanup().await;
        drop(_permit);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::init_from_env().expect("failed to load configuration from env");

    let cleaner = Cleaner::new();
    let cleanup_loop = Box::pin(cleanup_loop(cleaner, config.cleanup_interval_secs));

    let recorder_handle = metrics::setup_metrics_recorder();
    let app = handlers::app(Some(recorder_handle));
    let http_server = Box::pin(listen(app, config.bind()));

    match select(http_server, cleanup_loop).await {
        Either::Left((listen_result, _)) => match listen_result {
            Ok(_) => {}
            Err(e) => tracing::error!("failed to start hook-janitor http server, {}", e),
        },
        Either::Right((cleanup_result, _)) => match cleanup_result {
            Ok(_) => {}
            Err(e) => tracing::error!("hook-janitor cleanup task exited, {}", e),
        },
    };
}
