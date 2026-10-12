pub mod body;

pub mod conn;

pub mod client;
pub mod server;

/// A boxed error.
pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub const PORT: u16 = 3000;
pub const ADDR: std::net::SocketAddr = std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::LOCALHOST,
    PORT,
));
