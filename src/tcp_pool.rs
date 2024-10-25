use super::udp2tcp::Error;
use std::net::SocketAddr;
use tokio::net::{TcpSocket, TcpStream};

pub struct TcpPool {
    sockets: Vec<TcpSocket>,
}

pub struct TcpStreamPool {
    streams: Vec<TcpStream>,
}

pub struct TcpReadPool {
    reads: Vec<TcpReadHalf>,
}

pub struct TcpWritePool {
    writes: Vec<TcpWriteHalf>,
}

impl TcpPool {
    pub fn new(
        tcp_forward_addr: &SocketAddr,
        tcp_options: &crate::TcpOptions,
        size: usize,
    ) -> Result<Self, Error> {
        let mut sockets = Vec::with_capacity(size);
        match tcp_forward_addr {
            SocketAddr::V4(..) => {
                for _ in 0..size {
                    let tcp_socket = TcpSocket::new_v4().map_err(Error::CreateTcpSocket)?;
                    crate::tcp_options::apply(&tcp_socket, &tcp_options)
                        .map_err(Error::ApplyTcpOptions)?;
                    sockets.push(tcp_socket);
                }
            }
            SocketAddr::V6(..) => {
                for _ in 0..size {
                    let tcp_socket = TcpSocket::new_v6().map_err(Error::CreateTcpSocket)?;
                    crate::tcp_options::apply(&tcp_socket, &tcp_options)
                        .map_err(Error::ApplyTcpOptions)?;
                    sockets.push(tcp_socket);
                }
            }
        }

        Ok(Self { sockets })
    }
    pub async fn connect(self, addr: SocketAddr) -> Result<TcpStreamPool, Error> {
        let mut streams = Vec::with_capacity(self.sockets.len());
        for socket in self.sockets {
            streams.push(socket.connect(addr).await.map_err(Error::ConnectTcp)?)
        }

        Ok(TcpStreamPool { streams })
    }
}

impl TcpStreamPool {
    pub fn set_nodelay(self, nodelay: bool) -> Result<(), Error> {
        for stream in self.streams {
            crate::tcp_options::set_nodelay(&stream, nodelay).map_err(Error::ApplyTcpOptions)?;
        }
        Ok(())
    }
}
