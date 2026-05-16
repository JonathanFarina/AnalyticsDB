use std::convert::Infallible;
use std::net::SocketAddr;

use anyhow::Result;
use hyper::body::Body;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use tokio::sync::watch;
use tracing::{debug, error};

/// Starts a simple HTTP health server on the given address.
///
/// `ready_rx` receives a boolean indicating whether the server is ready.
/// - `/healthz` always returns 200 (liveness).
/// - `/readyz` returns 200 if ready, 503 otherwise.
pub fn start_health_server(
    addr: SocketAddr,
    ready_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let make_svc = make_service_fn(move |_conn| {
            let ready_rx = ready_rx.clone();
            Ok::<_, Infallible>(service_fn(move |req| {
                let ready_rx = ready_rx.clone();
                async move {
                    let path = req.uri().path();
                    let (status, body) = if path == "/healthz" {
                        // Liveness: always healthy if the process is running.
                        (hyper::StatusCode::OK, "OK\n".to_string())
                    } else if path == "/readyz" {
                        // Readiness: check if the engine is ready.
                        let ready = *ready_rx.borrow();
                        if ready {
                            (hyper::StatusCode::OK, "OK\n".to_string())
                        } else {
                            (hyper::StatusCode::SERVICE_UNAVAILABLE, "NOT READY\n".to_string())
                        }
                    } else {
                        (hyper::StatusCode::NOT_FOUND, "NOT FOUND\n".to_string())
                    };
                    debug!("Health check {} -> {}", path, status);
                    Ok::<_, Infallible>(
                        hyper::Response::builder()
                            .status(status)
                            .header("Content-Type", "text/plain")
                            .body(Body::from(body))
                            .unwrap(),
                    )
                }
            }))
        });

        let server = Server::bind(&addr).serve(make_svc);
        debug!("Health server listening on {}", addr);
        if let Err(e) = server.await {
            error!("Health server error: {}", e);
        }
    })
}
