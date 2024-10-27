use crate::tcp_pool::TcpPool;
use crate::udp2tcp::Error;

use super::forward_traffic::process_tcp2udp;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::{
    io::{self},
    net::{tcp::OwnedWriteHalf, TcpSocket, TcpStream},
};

pub struct TcpPoolClient {
    size: usize,
    addr: SocketAddr,
    tcp_option: crate::TcpOptions,
    writer_counter: AtomicUsize,
    write_streams: Vec<Arc<Mutex<OwnedWriteHalf>>>,
    read_channel: Option<RefCell<mpsc::Receiver<Vec<u8>>>>,
}

pub struct TcpPoolClientWriteHalf {
    size: usize,
    writer_counter: AtomicUsize,
    write_streams: Vec<Arc<Mutex<OwnedWriteHalf>>>,
}

impl TcpPoolClient {
    pub fn new(size: usize, addr: &SocketAddr, tcp_option: &crate::TcpOptions) -> Self {
        Self {
            size,
            addr: addr.clone(),
            tcp_option: tcp_option.clone(),
            writer_counter: AtomicUsize::new(0),
            write_streams: Vec::with_capacity(size),
            read_channel: None,
        }
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut read_streams = Vec::with_capacity(self.size);
        for _ in 0..self.size {
            let tcp_stream = self.connect_one().await?;
            let (read_stream, write_stream) = tcp_stream.into_split();
            read_streams.push(read_stream);
            self.write_streams.push(Arc::new(Mutex::new(write_stream)));
        }

        let (tx, rx) = mpsc::channel(1024);
        for read_stream in read_streams {
            let tx = tx.clone();
            let tcp_option = self.tcp_option.clone();
            tokio::spawn(async move {
                process_tcp2udp(read_stream, &tx, tcp_option.recv_timeout).await;
            });
        }

        self.read_channel = Some(RefCell::new(rx));
        Ok(())
    }

    async fn connect_one(&self) -> Result<TcpStream, Error> {
        let tcp_socket = match self.addr {
            SocketAddr::V4(..) => TcpSocket::new_v4().map_err(Error::CreateTcpSocket)?,
            SocketAddr::V6(..) => TcpSocket::new_v6().map_err(Error::CreateTcpSocket)?,
        };
        crate::tcp_options::apply(&tcp_socket, &self.tcp_option).map_err(Error::ApplyTcpOptions)?;
        let tcp_stream = tcp_socket
            .connect(self.addr)
            .await
            .map_err(Error::ConnectTcp)?;
        crate::tcp_options::set_nodelay(&tcp_stream, true).map_err(Error::ApplyTcpOptions)?;
        Ok(tcp_stream)
    }

    pub async fn read(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if let Some(rx_cell) = self.read_channel.as_ref() {
            let mut rx = rx_cell.borrow_mut();
            let buf = rx.recv().await.unwrap_or(Vec::new());
            Ok(buf)
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Receiver not Available",
            )))
        }
    }
}

impl TcpPool for TcpPoolClient {
    async fn write_all(&self, buf: Vec<u8>) -> io::Result<()> {
        // try all streams times
        for _ in 0..self.size {
            let selected = self.writer_counter.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut write_stream) = self.write_streams[selected % self.size].try_lock() {
                write_stream.write_all(buf.as_slice()).await.map_err(|e| {
                    // todo: notify daemon thread to reconnect the stream
                    e
                })?;
                return Ok(());
            }
        }
        // too many packets to send just drop packet
        Ok(())
    }
}

