use poll_progress::{ADDR, Error, conn, server};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let listener = conn::tcp::TcpListener::listen(ADDR)?;
    server::run_server(listener).await?;
    Ok(())
}
