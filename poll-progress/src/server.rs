use {
    crate::{ADDR, Error, body::IterBody, conn::Accept},
    http_body_util::combinators::BoxBody,
    hyper::{Request, Response, body::Bytes, service::Service},
    hyper_util::rt::{TokioExecutor, TokioTimer},
    std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    },
    tokio::time::interval,
    tracing::info,
};

pub async fn run_server<A>(listener: A) -> Result<(), Error>
where
    A: Accept<Error = Error>,
    A::Stream: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
{
    let svc = Svc::new();
    svc.spawn_metrics_logging();
    info!(%ADDR, "spawned metrics worker");

    loop {
        info!(%ADDR, "awaiting connections");
        let stream = listener.accept().await?;

        let fut = {
            let mut server = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
            server.max_frame_size(1024 * 16 /*16kb*/);
            server.adaptive_window(false);
            server.max_send_buf_size(1024 * 8 /*8kb*/);
            server.timer(TokioTimer::new());

            // server.writev(false);
            //server.max_buf_size(8192);

            server.serve_connection(stream, svc.clone())
        };

        tokio::spawn(fut);
    }
}

#[derive(Clone, Default)]
struct Svc {
    reqs_received: Arc<AtomicU32>,
    bodies_finished: Arc<AtomicU32>,
    bodies_dropped: Arc<AtomicU32>,
}

// ===== impl Svc =====

type SvcFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

impl Svc {
    const CHUNK: Bytes = Bytes::from_static(&[0_u8; 1024]);
    const COUNT: usize = 256;
    const TIMEOUT: Duration = Duration::from_millis(250);
}

impl<B> Service<Request<B>> for Svc {
    type Response = Response<BoxBody<Bytes, Error>>;
    type Error = Error;
    type Future = SvcFuture<Self::Response, Self::Error>;
    fn call(&self, _: Request<B>) -> Self::Future {
        info!("received request");
        self.reqs_received.fetch_add(1, Ordering::Relaxed);

        let body = self.payload();
        let rsp = Response::builder().status(200).body(body).unwrap();
        let fut = async move { Ok(rsp) };

        Box::pin(fut)
    }
}

impl Svc {
    fn new() -> Self {
        Default::default()
    }

    fn payload(&self) -> BoxBody<Bytes, Error> {
        let Self {
            reqs_received: _,
            bodies_finished,
            bodies_dropped,
        } = self;

        use {crate::body::BodyExt as _, http_body_util::BodyExt as _};
        let data = std::iter::repeat_n(Self::CHUNK, Self::COUNT);
        IterBody::new(data)
            .with_pending_injections()
            .with_metrics(bodies_finished.clone(), bodies_dropped.clone())
            .with_timeout(interval(Self::TIMEOUT))
            .boxed()
    }

    fn spawn_metrics_logging(&self) {
        const INTERVAL: Duration = Duration::from_secs(5);

        let Self {
            reqs_received,
            bodies_finished,
            bodies_dropped,
        } = self.clone();

        let worker = async move {
            let mut interval = interval(INTERVAL);
            interval.tick().await;

            loop {
                let reqs_received = reqs_received.load(Ordering::Relaxed);
                let bodies_finished = bodies_finished.load(Ordering::Relaxed);
                let bodies_dropped = bodies_dropped.load(Ordering::Relaxed);
                info!(%reqs_received, %bodies_finished, %bodies_dropped);

                interval.tick().await;
            }
        };

        info!("spawning metrics worker");
        tokio::spawn(worker);
    }
}
