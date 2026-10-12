use {
    crate::Error,
    hyper::body::{Body, Bytes, Frame},
    pin_project::{pin_project, pinned_drop},
    std::{
        pin::{self, Pin},
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        task::{self, Poll, ready},
    },
    tokio::time::Interval,
    tracing::info,
};

#[pin_project(project = IterBodyProj)]
pub struct IterBody<I> {
    iter: I,
    yielded: u32,
}

#[pin_project(project = WithMetricsProj, PinnedDrop)]
pub struct WithMetrics<B> {
    #[pin]
    inner: B,
    finished: Arc<AtomicU32>,
    dropped: Arc<AtomicU32>,
}

#[pin_project(project = WithTimeoutProj)]
pub struct WithTimeout<B> {
    #[pin]
    timeout: Interval,
    #[pin]
    inner: B,
}

#[pin_project(project = WithPendingProj)]
pub struct WithPending<B> {
    #[pin]
    inner: B,
    will_poll: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("body timed out between frames")]
pub struct WithTimeoutError;

pub trait BodyExt {
    fn with_metrics(self, finished: Arc<AtomicU32>, dropped: Arc<AtomicU32>) -> WithMetrics<Self>
    where
        Self: Sized,
    {
        WithMetrics {
            finished,
            dropped,
            inner: self,
        }
    }

    fn with_timeout(self, mut interval: Interval) -> WithTimeout<Self>
    where
        Self: Sized,
    {
        interval.reset();
        WithTimeout {
            timeout: interval,
            inner: self,
        }
    }

    fn with_pending_injections(self) -> WithPending<Self>
    where
        Self: Sized,
    {
        WithPending {
            inner: self,
            will_poll: false,
        }
    }
}

// ===== impl BodyExt =====

impl<B> BodyExt for B where B: hyper::body::Body {}

// ===== impl IterBody =====

impl<I> IterBody<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            yielded: Default::default(),
        }
    }
}

impl<I> Body for IterBody<I>
where
    I: Iterator<Item = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        self: pin::Pin<&mut Self>,
        _: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let IterBodyProj { iter, yielded } = self.project();

        let frame = iter.next().map(Frame::data).map(Ok);
        *yielded += 1;
        info!(%yielded, "yielding next frame");

        Poll::Ready(frame)
    }
}

#[cfg(test)]
mod iter_body_test {
    use {super::*, http_body_util::BodyExt};

    #[tokio::test]
    async fn works() {
        let mut body = {
            let range = 10..20;
            let iter = range.map(|i| Bytes::from(i.to_string()));
            IterBody::new(iter)
        };

        assert_eq!(body.yielded, 0);
        let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(frame, "10");
        assert_eq!(body.yielded, 1);
        let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(frame, "11");
        assert_eq!(body.yielded, 2);
    }
}

// ===== impl WithTimeout =====

impl<B> Body for WithTimeout<B>
where
    B: Body,
    B::Error: From<WithTimeoutError>,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let WithTimeoutProj { mut timeout, inner } = self.project();

        match timeout.poll_tick(cx) {
            Poll::Pending => timeout.reset(),
            Poll::Ready(deadline) => {
                let oversleep = tokio::time::Instant::now().duration_since(deadline);
                tracing::error!(
                    oversleep.ms = oversleep.as_millis(),
                    "body timed out while reading"
                );
                return Poll::Ready(Some(Err(WithTimeoutError.into())));
            }
        }

        inner.poll_frame(cx)
    }
}

#[cfg(test)]
mod with_timeout_test {
    use {super::*, http_body_util::BodyExt, std::time::Duration, tokio::time::interval};

    #[tokio::test]
    async fn returns_error_after_timeout() {
        const INTERVAL: Duration = Duration::from_secs(1);

        let mut body = {
            let iter = std::iter::repeat(Bytes::from("a"));
            let inner = IterBody::new(iter);
            WithTimeout {
                timeout: interval(INTERVAL),
                inner,
            }
        };

        let a = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(a, "a");
        let a = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(a, "a");

        tokio::time::sleep(INTERVAL * 2).await;
        body.frame()
            .await
            .unwrap()
            .unwrap_err()
            .downcast::<WithTimeoutError>()
            .expect("is a timeout error");
    }

    #[test]
    fn error_is_compatible() {
        fn _from_with_timeout_error<E>()
        where
            E: From<WithTimeoutError>,
        {
        }
        _from_with_timeout_error::<Error>();
    }
}

// ===== impl WithMetrics =====

impl<B> Body for WithMetrics<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let WithMetricsProj {
            inner,
            finished,
            dropped: _,
        } = self.project();
        let frame = ready!(inner.poll_frame(cx));

        match &frame {
            Some(Ok(_)) => {}
            None | Some(Err(_)) => {
                let finished = finished.fetch_add(1, Ordering::Relaxed);
                info!(%finished, "body is finished");
            }
        }

        Poll::Ready(frame)
    }
}

#[pinned_drop]
impl<B> PinnedDrop for WithMetrics<B> {
    fn drop(self: Pin<&mut Self>) {
        let dropped = self.dropped.fetch_add(1, Ordering::Relaxed);
        info!(%dropped, "body is dropped");
    }
}

#[cfg(test)]
mod with_metrics_test {
    use {
        super::*,
        http_body_util::{BodyExt, Full},
    };

    #[test]
    fn marks_dropped() {
        let finished = Arc::new(AtomicU32::default());
        let dropped = Arc::new(AtomicU32::default());

        let body = {
            let inner = Full::new(Bytes::from_static(b"poll me"));
            WithMetrics {
                inner,
                finished: finished.clone(),
                dropped: dropped.clone(),
            }
        };

        assert_eq!(finished.load(Ordering::Relaxed), 0);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(body);
        assert_eq!(finished.load(Ordering::Relaxed), 0);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn marks_finished() {
        let finished = Arc::new(AtomicU32::default());
        let dropped = Arc::new(AtomicU32::default());

        let mut body = {
            let inner = Full::new(Bytes::from_static(b"poll me"));
            WithMetrics {
                inner,
                finished: finished.clone(),
                dropped: dropped.clone(),
            }
        };

        assert_eq!(finished.load(Ordering::Relaxed), 0);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        let _: Frame<Bytes> = body.frame().await.unwrap().unwrap();
        assert_eq!(finished.load(Ordering::Relaxed), 0);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        let None = body.frame().await else {
            panic!("unexpected frame")
        };
        assert_eq!(finished.load(Ordering::Relaxed), 1);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }
}

// ===== impl WithPending =====

impl<B> Body for WithPending<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let WithPendingProj { inner, will_poll } = self.project();

        let poll = if *will_poll {
            inner.poll_frame(cx)
        } else {
            let waker = cx.waker();
            waker.wake_by_ref();
            Poll::Pending
        };
        *will_poll = !*will_poll;

        poll
    }
}

#[cfg(test)]
mod pending_body_test {
    use {super::*, std::task::Context};

    #[tokio::test]
    async fn pending_is_injected() {
        let body = {
            let range = 10..20;
            let iter = range.map(|i| Bytes::from(i.to_string()));
            WithPending {
                inner: IterBody::new(iter),
                will_poll: false,
            }
        };
        let mut body = std::pin::pin!(body);

        let waker = futures::task::noop_waker_ref();
        let mut cx = Context::from_waker(waker);

        let poll = body.as_mut().poll_frame(&mut cx);
        assert!(poll.is_pending(), "should be pending");
        let poll = body.as_mut().poll_frame(&mut cx);
        assert!(poll.is_ready(), "should be pending");
        let poll = body.as_mut().poll_frame(&mut cx);
        assert!(poll.is_pending(), "should be pending");
        let poll = body.poll_frame(&mut cx);
        assert!(poll.is_ready(), "should be pending");
    }
}
