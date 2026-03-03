//! Graze API server binary.

use std::sync::Arc;

use tokio::signal;
use tokio::sync::broadcast;
use tracing::{debug, info, Level};
use tracing_subscriber::EnvFilter;

use graze_api::algorithm::{LinkLonkAlgorithm, ThompsonLearner};
use graze_api::api::create_router;
use graze_api::config::Config;
use graze_api::impression_queue;
use graze_api::interaction_queue;
use graze_api::metrics::Metrics;
use graze_api::ml::OnnxRanker;
use graze_api::AppState;
use graze_common::{
    maybe_run_metrics_server, ClickHouseConfig, ClickHouseImpressionWriter,
    ClickHouseInteractionWriter, InteractionsClient, InteractionsConfig, NoOpImpressionWriter,
    NoOpInteractionWriter, RedisClient, RedisConfig, SpecialPostsClient, SpecialPostsSource,
    UriInterner,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .json()
        .init();

    info!("Starting Graze API server");

    // Load configuration
    let config = Config::from_env();
    let bind_addr = format!("{}:{}", config.http_host, config.http_port);

    // Capture metrics port before wrapping config in Arc
    let metrics_port = config.metrics_port;
    let metrics_host = config.http_host.clone();

    // Log configuration (redact sensitive values)
    debug!(
        redis_url = %redact_url(&config.redis_url),
        redis_pool_size = config.redis_pool_size,
        redis_connect_max_retries = config.redis_connect_max_retries,
        redis_connect_initial_delay_ms = config.redis_connect_initial_delay_ms,
        http_host = %config.http_host,
        http_port = config.http_port,
        metrics_port = metrics_port,
        "Configuration loaded"
    );

    // Log read-only mode warning
    if config.read_only_mode {
        info!("RUNNING IN READ-ONLY MODE - Redis writes will be skipped and logged");
    }

    // Create Redis client
    let redis_config = config.redis_config();
    let redis = Arc::new(RedisClient::new(&redis_config).await?);
    info!("Connected to Redis");

    // Optional Redis client for post-render logging (log_tasks queue). Skip quietly when unset.
    let redis_requests_logger = match &config.redis_requests_logger_url {
        Some(url) => {
            let logger_config = RedisConfig {
                url: url.clone(),
                pool_size: 4,
                connect_max_retries: config.redis_connect_max_retries,
                connect_initial_delay_ms: config.redis_connect_initial_delay_ms,
            };
            match RedisClient::new(&logger_config).await {
                Ok(client) => {
                    debug!(
                        redis_requests_logger = "configured",
                        "Requests logger Redis connected"
                    );
                    Some(Arc::new(client))
                }
                Err(e) => {
                    tracing::warn!(error = %e, "REDIS_REQUESTS_LOGGER set but connection failed; post-render logging disabled");
                    None
                }
            }
        }
        None => None,
    };

    // Create services
    let config = Arc::new(config);
    let interner = Arc::new(UriInterner::new(redis.clone()));
    let special_posts_source = if config.special_posts_source.to_lowercase().trim() == "local" {
        SpecialPostsSource::Local
    } else {
        SpecialPostsSource::Remote {
            api_base_url: config.special_posts_api_base.clone(),
        }
    };
    let special_posts = Arc::new(SpecialPostsClient::new(redis.clone(), special_posts_source));
    let metrics = Arc::new(Metrics::new());

    // Create Thompson learner for adaptive parameter optimization
    let thompson = Arc::new(ThompsonLearner::new());
    info!(
        holdout_rate = 0.10,
        exploration_prob = 0.05,
        "Thompson learner initialized"
    );

    // Create interaction writer backend (clickhouse or none)
    let interaction_writer: Arc<dyn graze_common::InteractionWriter> =
        if config.interactions_writer == "none" {
            Arc::new(NoOpInteractionWriter)
        } else {
            let ch_config = Arc::new(ClickHouseConfig {
                host: config.clickhouse_host.clone(),
                port: config.clickhouse_port,
                database: config.clickhouse_database.clone(),
                user: config.clickhouse_user.clone(),
                password: config.clickhouse_password.clone(),
                secure: config.clickhouse_secure,
            });
            Arc::new(ClickHouseInteractionWriter::new(ch_config))
        };

    let interactions_config = Arc::new(InteractionsConfig {
        clickhouse_host: config.clickhouse_host.clone(),
        clickhouse_port: config.clickhouse_port,
        clickhouse_user: config.clickhouse_user.clone(),
        clickhouse_password: config.clickhouse_password.clone(),
        clickhouse_database: config.clickhouse_database.clone(),
        clickhouse_secure: config.clickhouse_secure,
    });
    let interactions = Arc::new(InteractionsClient::new(
        interactions_config,
        redis.clone(),
        interaction_writer,
    ));
    info!(
        interactions_logging_enabled = config.interactions_logging_enabled,
        interactions_writer = %config.interactions_writer,
        "Interactions client initialized"
    );

    // Create interaction queue and worker when logging is enabled
    let (interaction_queue, worker_handle) = if config.interactions_logging_enabled {
        let (tx, rx) = tokio::sync::mpsc::channel(config.interactions_queue_capacity);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let shutdown_rx = shutdown_tx.subscribe();

        let worker_handle = tokio::spawn(interaction_queue::run_interaction_worker(
            rx,
            shutdown_rx,
            interactions.clone(),
            config.interactions_batch_interval_ms,
            config.interactions_batch_size,
        ));

        let queue_sender = interaction_queue::InteractionQueueSender::new(tx);
        let shutdown_tx_for_signal = shutdown_tx.clone();
        // Store shutdown_tx for the shutdown signal
        (
            Some(queue_sender),
            Some((worker_handle, shutdown_tx_for_signal)),
        )
    } else {
        (None, None)
    };

    // Load ONNX re-ranker when ML_RERANKER_ENABLED=true and ML_MODEL_PATH is set
    let ml_ranker: Option<Arc<OnnxRanker>> = if config.ml_reranker_enabled
        && !config.ml_model_path.is_empty()
    {
        match OnnxRanker::load(
            &config.ml_model_path,
            config.ml_reranker_alpha,
            config.ml_reranker_beta,
        ) {
            Ok(ranker) => {
                info!(
                    path = %config.ml_model_path,
                    alpha = config.ml_reranker_alpha,
                    beta = config.ml_reranker_beta,
                    "ONNX re-ranker loaded"
                );
                Some(Arc::new(ranker))
            }
            Err(e) => {
                tracing::error!(error = %e, path = %config.ml_model_path, "Failed to load ONNX model; re-ranker disabled");
                None
            }
        }
    } else {
        if config.ml_reranker_enabled {
            tracing::warn!(
                "ML_RERANKER_ENABLED=true but ML_MODEL_PATH is empty; re-ranker disabled"
            );
        }
        None
    };

    // Create impression queue and worker when ML impressions are enabled
    let (impression_queue, impression_worker_handle) = if config.ml_impressions_enabled {
        let ch_config = Arc::new(ClickHouseConfig {
            host: config.clickhouse_host.clone(),
            port: config.clickhouse_port,
            database: config.clickhouse_database.clone(),
            user: config.clickhouse_user.clone(),
            password: config.clickhouse_password.clone(),
            secure: config.clickhouse_secure,
        });
        let impression_writer: Arc<dyn graze_common::ImpressionWriter> =
            if config.interactions_writer == "none" {
                Arc::new(NoOpImpressionWriter)
            } else {
                Arc::new(ClickHouseImpressionWriter::new(ch_config))
            };

        let (tx, rx) = tokio::sync::mpsc::channel(config.ml_impressions_queue_capacity);
        let (imp_shutdown_tx, _) = broadcast::channel::<()>(1);
        let imp_shutdown_rx = imp_shutdown_tx.subscribe();

        let handle = tokio::spawn(impression_queue::run_impression_worker(
            rx,
            imp_shutdown_rx,
            impression_writer,
            config.ml_impressions_batch_interval_ms,
            config.ml_impressions_batch_size,
        ));

        let sender = impression_queue::ImpressionQueueSender::new(tx);
        info!(
            queue_capacity = config.ml_impressions_queue_capacity,
            batch_size = config.ml_impressions_batch_size,
            batch_interval_ms = config.ml_impressions_batch_interval_ms,
            "ML impression queue started"
        );
        (Some(sender), Some((handle, imp_shutdown_tx)))
    } else {
        (None, None)
    };

    // Create application state
    let state = Arc::new(AppState {
        config: config.clone(),
        redis: redis.clone(),
        interner: interner.clone(),
        algorithm: Arc::new(LinkLonkAlgorithm::new(
            redis.clone(),
            interner.clone(),
            config.clone(),
        )),
        special_posts,
        metrics,
        thompson,
        interactions,
        interaction_queue,
        redis_requests_logger,
        ml_ranker,
        impression_queue,
    });

    // Build router
    let app = create_router(state.clone());

    // Start server
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!(address = %bind_addr, "API server listening");

    // Shutdown signal: waits for ctrl_c, then notifies all workers
    let interaction_shutdown_tx = worker_handle.as_ref().map(|(_, tx)| tx.clone());
    let impression_shutdown_tx = impression_worker_handle.as_ref().map(|(_, tx)| tx.clone());
    let shutdown_future = async move {
        shutdown_signal().await;
        if let Some(tx) = interaction_shutdown_tx {
            let _ = tx.send(());
        }
        if let Some(tx) = impression_shutdown_tx {
            let _ = tx.send(());
        }
    };

    // Run API server and optional metrics server concurrently
    tokio::select! {
        result = axum::serve(listener, app).with_graceful_shutdown(shutdown_future) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "API server error");
            }
        }
        result = maybe_run_metrics_server(metrics_port, &metrics_host, state.metrics.clone()) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Metrics server error");
            }
        }
    }

    // Wait for workers to flush remaining and exit
    if let Some((handle, _)) = worker_handle {
        let _ = handle.await;
    }
    if let Some((handle, _)) = impression_worker_handle {
        let _ = handle.await;
    }

    info!("Server shutdown complete");
    Ok(())
}

/// Redact password from URL for safe logging.
fn redact_url(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3];
            let after_at = &url[at_pos..];
            return format!("{}[REDACTED]{}", scheme, after_at);
        }
    }
    url.to_string()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}
