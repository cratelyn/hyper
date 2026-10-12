use poll_progress::{ADDR, Error, client, conn};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    let socket = conn::tcp::TcpSocket::new(ADDR).await?;
    client::send_request(socket).await
}
