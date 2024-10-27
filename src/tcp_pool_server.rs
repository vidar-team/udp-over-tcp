use super::forward_traffic::process_tcp2udp;
use crate::exponential_backoff::ExponentialBackoff;
use crate::logging::Redact;
use crate::tcp2udp::{create_listening_socket, Tcp2UdpError};
use crate::tcp_pool::TcpPool;
use err_context::{BoxedErrorExt as _, ErrorExt as _, ResultExt as _};
use futures::{FutureExt, TryFutureExt};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{tcp, TcpListener};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio::{
    io::{self},
    net::{tcp::OwnedWriteHalf, TcpSocket, TcpStream},
};

#[path = "statsd.rs"]
mod statsd;

const MAX_CONNECTIONS: usize = 256;

pub struct TcpPoolServer {
    listen_addr: Vec<SocketAddr>,
    tcp_option: crate::TcpOptions,
    writer_counter: AtomicUsize,

    rx_conn: RefCell<mpsc::Receiver<OwnedWriteHalf>>,
    tx_conn: Arc<mpsc::Sender<OwnedWriteHalf>>,

    rx_content: RefCell<mpsc::Receiver<Vec<u8>>>,
    tx_content: Arc<mpsc::Sender<Vec<u8>>>,
}

impl TcpPoolServer {
    pub fn new(listen_addr: Vec<SocketAddr>, tcp_option: crate::TcpOptions) -> Self {
        let (tx, rx) = mpsc::channel(MAX_CONNECTIONS);
        let (tx_content, rx_content) = mpsc::channel(1024);
        Self {
            listen_addr,
            tcp_option,
            writer_counter: AtomicUsize::new(0),
            rx_conn: RefCell::new(rx),
            tx_conn: Arc::new(tx),
            rx_content: RefCell::new(rx_content),
            tx_content: Arc::new(tx_content),
        }
    }

    pub fn run(&self) -> Result<(), Tcp2UdpError> {
        // stat
        #[cfg(not(feature = "statsd"))]
        let statsd = Arc::new(statsd::StatsdMetrics::dummy());
        #[cfg(feature = "statsd")]
        let statsd = Arc::new(match options.statsd_host {
            None => statsd::StatsdMetrics::dummy(),
            Some(statsd_host) => statsd::StatsdMetrics::real(statsd_host)
                .map_err(Tcp2UdpError::CreateStatsdClient)?,
        });

        // let mut join_handles = Vec::with_capacity(self.listen_addr.len());
        for tcp_listen_addr in &self.listen_addr {
            let tcp_listener = create_listening_socket(tcp_listen_addr.clone(), &self.tcp_option)?;
            log::info!("Listening on {}/TCP", tcp_listener.local_addr().unwrap());

            let tcp_recv_timeout = self.tcp_option.recv_timeout;
            let tcp_nodelay = self.tcp_option.nodelay;
            let statsd = Arc::clone(&statsd);
            tokio::spawn(accept_tcp_listener(
                Arc::clone(&self.tx_conn),
                Arc::clone(&self.tx_content),
                tcp_listener,
                tcp_recv_timeout,
                tcp_nodelay,
                statsd,
            ));
        }
        // futures::future::join_all(join_handles).await;
        Ok(())
    }
}

impl TcpPool for TcpPoolServer {
    async fn write_all(&self, buffer: Vec<u8>) -> Result<(), std::io::Error> {
        match self.rx_conn.borrow_mut().try_recv() {
            Ok(mut w) => {
                let tx_conn = Arc::clone(&self.tx_conn);
                tokio::spawn(async move {
                    if let Err(err) = w.write_all(buffer.as_slice()).await {
                        log::error!("Error: {}", err);
                        return;
                    }
                    tx_conn.send(w).await.unwrap();
                });
                Ok(())
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(()),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Receiver not Available",
            )),
        }
    }
}

async fn accept_tcp_listener(
    tx_conn: Arc<mpsc::Sender<OwnedWriteHalf>>,
    tx_content: Arc<mpsc::Sender<Vec<u8>>>,
    tcp_listener: TcpListener,
    tcp_recv_timeout: Option<Duration>,
    tcp_nodelay: bool,
    statsd: Arc<statsd::StatsdMetrics>,
) -> ! {
    let mut cooldown =
        ExponentialBackoff::new(Duration::from_millis(50), Duration::from_millis(5000));
    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, tcp_peer_addr)) => {
                log::debug!("Incoming connection from {}/TCP", Redact(tcp_peer_addr));
                if let Err(error) = crate::tcp_options::set_nodelay(&tcp_stream, tcp_nodelay) {
                    log::error!("Error: {}", error.display("\nCaused by: "));
                }
                let statsd = statsd.clone();
                let (r, w) = tcp_stream.into_split();
                if let Err(error) = tx_conn.try_send(w) {
                    match error {
                        mpsc::error::TrySendError::Full(_) => {
                            log::error!("Error: Connection pool is full");
                            continue;
                        }
                        mpsc::error::TrySendError::Closed(_) => {
                            panic!("Connection pool is closed");
                        }
                    }
                }
                // tcp->buffer
                let tx_content = tx_content.clone();
                tokio::spawn(async move {
                    statsd.incr_connections();
                    if let Err(err) = process_tcp2udp(r, &tx_content, tcp_recv_timeout).await {
                        log::error!("Error: {}", err.display("\nCaused by: "));
                    }
                    statsd.decr_connections();
                });
                cooldown.reset();
            }
            Err(error) => {
                log::error!("Error when accepting incoming TCP connection: {}", error);

                statsd.accept_error();

                // If the process runs out of file descriptors, it will fail to accept a socket.
                // But that socket will also remain in the queue, so it will fail again immediately.
                // This will busy loop consuming the CPU and filling any logs. To prevent this,
                // delay between failed socket accept operations.
                sleep(cooldown.next_delay()).await;
            }
        }
    }
}
