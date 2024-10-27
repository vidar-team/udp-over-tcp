use crate::udp2tcp::Error;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{
    io::{self},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpSocket, TcpStream,
    },
};

pub struct TcpPoolClient {
    size: usize,
    addr: SocketAddr,
    tcp_option: crate::TcpOptions,
    reader_counter: AtomicUsize,
    writer_counter: AtomicUsize,
    read_streams: Vec<Arc<Mutex<BufReader<OwnedReadHalf>>>>,
    write_streams: Vec<Arc<Mutex<OwnedWriteHalf>>>,
    read_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl TcpPoolClient {
    pub fn new(
        size: usize,
        addr: &SocketAddr,
        tcp_option: &crate::TcpOptions,
    ) -> Result<Self, Error> {
        Ok(Self {
            size,
            addr: addr.clone(),
            tcp_option: tcp_option.clone(),
            reader_counter: AtomicUsize::new(0),
            writer_counter: AtomicUsize::new(0),
            read_streams: Vec::with_capacity(size),
            write_streams: Vec::with_capacity(size),
            read_queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        for _ in 0..self.size {
            let tcp_stream = self.connect_one().await?;
            let (read_stream, write_stream) = tcp_stream.into_split();
            self.read_streams
                .push(Arc::new(Mutex::new(BufReader::new(read_stream))));
            self.write_streams.push(Arc::new(Mutex::new(write_stream)));
        }
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

    pub async fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        // try all streams times
        for _ in 0..self.size {
            let selected = self.writer_counter.fetch_add(1, Ordering::SeqCst);
            if let Ok(mut write_stream) = self.write_streams[selected % self.size].try_lock() {
                write_stream.write_all(buf).await.map_err(|e| {
                    // todo: notify daemon thread to reconnect the stream
                    e
                })?;
            }
        }
        // too many packets to send just drop packet
        Ok(())
    }

    pub fn run_read(&self) {
        for index in 0..self.size {
            self.read_one(index);
        }
    }

    pub async fn read_one(&self, index: usize) {
        loop {
            let mut read_stream = self.read_streams[index].lock().unwrap();
            loop {
                let size = read_stream.read_u32().await.unwrap();
                let mut buf = vec![0u8; size as usize];
                read_stream.read_exact(&mut buf).await.unwrap();
                self.read_queue.lock().unwrap().push_back(buf);
            }
        }
    }
}
