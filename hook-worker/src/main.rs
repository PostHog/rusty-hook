//! Consume `PgQueue` jobs to run webhook calls.
use axum::routing::get;
use axum::Router;
use envconfig::Envconfig;
use std::future::ready;

use hook_common::health::HealthRegistry;
use hook_common::{
    metrics::serve, metrics::setup_metrics_routes, pgqueue::PgQueue, retry::RetryPolicy,
};
use hook_worker::config::Config;
use hook_worker::error::WorkerError;
use hook_worker::worker::WebhookWorker;

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    tracing_subscriber::fmt::init();

    let config = Config::init_from_env().expect("Invalid configuration:");

    let liveness = HealthRegistry::new("liveness");
    let worker_liveness = liveness
        .register("worker".to_string(), time::Duration::seconds(60)) // TODO: compute the value from worker params
        .await;

    let retry_policy_builder = RetryPolicy::build(
        config.retry_policy.backoff_coefficient,
        config.retry_policy.initial_interval.0,
    )
    .maximum_interval(config.retry_policy.maximum_interval.0);

    let retry_policy_builder = match &config.retry_policy.retry_queue_name {
        Some(retry_queue_name) if !retry_queue_name.is_empty() => {
            retry_policy_builder.queue(retry_queue_name)
        }
        _ => retry_policy_builder,
    };

    let queue = PgQueue::new(&config.queue_name, &config.database_url)
        .await
        .expect("failed to initialize queue");

    let worker = WebhookWorker::new(
        &config.worker_name,
        &queue,
        config.poll_interval.0,
        config.request_timeout.0,
        config.max_concurrent_jobs,
        retry_policy_builder.provide(),
        worker_liveness,
    );

    let router = Router::new()
        .route("/", get(index))
        .route("/_readiness", get(index))
        .route("/_liveness", get(move || ready(liveness.get_status())));
    let router = setup_metrics_routes(router);
    let bind = config.bind();
    tokio::task::spawn(async move {
        serve(router, &bind)
            .await
            .expect("failed to start serving metrics");
    });

    worker.run(config.transactional).await?;

    Ok(())
}

pub async fn index() -> &'static str {
    "rusty-hook worker"
}
