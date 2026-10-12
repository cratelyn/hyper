use {crate::Error, std::net::SocketAddr, tracing::info};

#[async_trait::async_trait]
pub trait Accept {
    type Stream;
    type Error;
    async fn accept(&self) -> Result<Self::Stream, Self::Error>;
}

#[async_trait::async_trait]
pub trait Connect {
    type Stream;
    type Error;
    async fn connect(self) -> Result<Self::Stream, Self::Error>;
}

pub mod tcp {
    use {super::*, hyper_util::rt::TokioIo, tokio::net::TcpStream};

    pub struct TcpListener(tokio::net::TcpListener);

    pub struct TcpSocket(tokio::net::TcpSocket, SocketAddr);

    // ===== impl TcpSocket =====

    impl TcpSocket {
        pub async fn new(addr: SocketAddr) -> Result<Self, Error> {
            let socket = tokio::net::TcpSocket::new_v4()?;
            socket.set_send_buffer_size(1024)?;
            socket.set_recv_buffer_size(1024 * 8)?;

            Ok(Self(socket, addr))
        }
    }

    #[async_trait::async_trait]
    impl Connect for TcpSocket {
        type Stream = TokioIo<TcpStream>;
        type Error = Error;
        async fn connect(self) -> Result<Self::Stream, Self::Error> {
            let Self(socket, addr) = self;
            let stream = socket.connect(addr).await?;
            info!(%addr, "connected to listener");
            let io = TokioIo::new(stream);
            Ok(io)
        }
    }

    // ===== impl TcpListener =====

    impl TcpListener {
        pub fn listen(addr: SocketAddr) -> Result<Self, Error> {
            let socket = tokio::net::TcpSocket::new_v4()?;
            socket.set_send_buffer_size(1024)?;
            socket.set_recv_buffer_size(1024)?;
            socket.bind(addr)?;

            let listener = socket.listen(1)?;
            info!(%addr, "bound listener");

            Ok(Self(listener))
        }
    }

    #[async_trait::async_trait]
    impl Accept for TcpListener {
        type Stream = TokioIo<TcpStream>;
        type Error = Error;
        async fn accept(&self) -> Result<Self::Stream, Self::Error> {
            let (stream, peer) = self.0.accept().await?;
            info!(%peer, "accepted connection");
            let io = TokioIo::new(stream);
            Ok(io)
        }
    }
}

pub mod duplex {
    use {
        super::*,
        hyper_util::rt::TokioIo,
        std::sync::{Arc, Mutex},
        tokio::io::DuplexStream,
    };

    pub struct DuplexListener {
        stream: Arc<Mutex<Option<DuplexStream>>>,
    }

    pub struct DuplexConnect {
        stream: Option<DuplexStream>,
    }

    pub fn connection(max_buf_size: usize) -> (DuplexConnect, DuplexListener) {
        let (client, server) = tokio::io::duplex(max_buf_size);
        let connect = DuplexConnect {
            stream: Some(client),
        };
        let listen = DuplexListener {
            stream: Arc::new(Mutex::new(Some(server))),
        };

        (connect, listen)
    }

    #[async_trait::async_trait]
    impl Connect for DuplexConnect {
        type Stream = TokioIo<DuplexStream>;
        type Error = Error;
        async fn connect(mut self) -> Result<Self::Stream, Self::Error> {
            let stream = self.stream.take().unwrap();
            Ok(TokioIo::new(stream))
        }
    }

    #[async_trait::async_trait]
    impl Accept for DuplexListener {
        type Stream = TokioIo<DuplexStream>;
        type Error = Error;
        async fn accept(&self) -> Result<Self::Stream, Self::Error> {
            let stream = self.stream.clone();
            let stream = stream.lock().unwrap().take().unwrap();
            Ok(TokioIo::new(stream))
        }
    }
}
