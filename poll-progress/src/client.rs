use {
    crate::{ADDR, Error, conn::Connect},
    http_body_util::{BodyExt, Full},
    hyper::body::Body as _,
    hyper::{Method, Request, body::Bytes},
    hyper_util::rt::TokioExecutor,
    std::time::Duration,
    tokio::time::interval,
    tracing::{info, trace},
};

pub async fn send_request<C>(connect: C) -> Result<(), Error>
where
    C: Connect<Error = Error>,
    C::Stream: hyper::rt::Read + hyper::rt::Write + Send + Unpin + 'static,
{
    let io = connect.connect().await?;

    let (mut tx, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(io)
        .await?;
    tokio::spawn(conn);

    let uri = format!("http://{ADDR}/");
    let body = Full::new(Bytes::from("request payload"));
    info!(req.uri = %uri, req.body = ?body, "preparing request");
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .body(body)
        .unwrap();
    let (rsp, mut body) = tx.send_request(req).await?.into_parts();
    info!(%rsp.status, "received response");

    const QUICK_READ: usize = 1024 * 16;
    let mut buf = vec![];
    let mut yielded = 0;

    #[allow(clippy::while_let_loop)]
    loop {
        match body.frame().await.transpose()? {
            Some(frame) => {
                yielded += 1;
                let bytes = frame.into_data().unwrap().to_vec();
                buf.extend(bytes);
                if buf.len() >= QUICK_READ {
                    break;
                }
            }
            None => break,
        }
    }

    tracing::info!(len = buf.len(), remaining = ?body.size_hint(), "read head of body");

    let mut poll_interval = interval(Duration::from_millis(100));
    poll_interval.tick().await;

    while let Some(frame) = body.frame().await.transpose()? {
        trace!(%yielded, "polling body");
        let bytes = frame.into_data().unwrap().to_vec();
        yielded += 1;
        info!(%yielded, "received frame");

        buf.extend(bytes);
        poll_interval.tick().await;
    }

    let body = String::from_utf8(buf).unwrap();
    info!(%body, "finished reading body");

    Ok(())
}
