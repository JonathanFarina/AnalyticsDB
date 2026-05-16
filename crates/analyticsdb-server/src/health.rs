use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::body::{Body, Bytes};
use hyper::service::Service;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, error};
use hyper_util::{
    rt::TokioIo,
    server::conn::auto::Builder,
};

/// Health service handler for liveness and readiness probes.
struct HealthService {
    ready_rx: watch::Receiver<bool>,
}

impl HealthService {
    fn new(ready_rx: watch::Receiver<bool>) -> Self {
        Self { ready_rx }
    }
}

impl Service<hyper::Request<hyper::body::Incoming>> for HealthService {
    type Response = hyper::Response<HttpBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let ready_rx = self.ready_rx.clone();
        Box::pin(async move {
            let path = req.uri().path();
            let (status, body) = if path == "/healthz" {
                (hyper::StatusCode::OK, "OK\n".to_string())
            } else if path == "/readyz" {
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
            let body_bytes = Bytes::from(body);
            Ok(hyper::Response::builder()
                .status(status)
                .header("Content-Type", "text/plain")
                .body(HttpBody::new(body_bytes))
                .unwrap())
        })
    }
}

/// A simple body type that wraps Bytes.
struct HttpBody {
    bytes: Option<Bytes>,
}

impl HttpBody {
    fn new(bytes: Bytes) -> Self {
        Self { bytes: Some(bytes) }
    }
}

impl Body for HttpBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if let Some(bytes) = this.bytes.take() {
            Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes))))
        } else {
            Poll::Ready(None)
        }
    }
}

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
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind health server to {}: {}", addr, e);
                return;
            }
        };
        debug!("Health server listening on {}", addr);
        let builder = Builder::new(hyper_util::rt::TokioExecutor::new());
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let ready_rx = ready_rx.clone();
                    let svc = HealthService::new(ready_rx);
                    let builder = builder.clone();
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        if let Err(e) = builder.serve_connection(io, svc).await {
                            debug!("Health connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Health server accept error: {}", e);
                }
            }
        }
    })
}
