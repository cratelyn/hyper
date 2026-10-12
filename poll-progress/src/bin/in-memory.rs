use {
    poll_progress::{Error, client, conn, server},
    tracing::{Instrument, error_span},
};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (socket, listener) = conn::duplex::connection(1024);

    let fut = async move { server::run_server(listener).await.unwrap() }
        .instrument(error_span!("server"));
    let _server = tokio::spawn(fut);

    client::send_request(socket).await.unwrap();

    Ok(())
}
