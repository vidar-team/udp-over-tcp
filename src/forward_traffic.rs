use crate::tcp_pool::TcpPool;
use crate::tcp_pool_client::TcpPoolClient;
use crate::NeverOkResult;
use err_context::BoxedErrorExt as _;
use err_context::ResultExt as _;
use futures::future::select;
use futures::pin_mut;
use std::convert::Infallible;
use std::future::Future;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf as TcpReadHalf, OwnedWriteHalf as TcpWriteHalf};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::timeout;

/// A UDP datagram header has a 16 bit field containing an unsigned integer
/// describing the length of the datagram (including the header itself).
/// The max value is 2^16 = 65536 bytes. But since that includes the
/// UDP header, this constant is 8 bytes more than any UDP socket
/// read operation would ever return. We are going to use that extra space
/// to store our 2 byte udp-over-tcp header.
pub const MAX_DATAGRAM_SIZE: usize = u16::MAX as usize;
pub const HEADER_LEN: usize = mem::size_of::<u16>();

/// Reads from `tcp_in` and extracts UDP datagrams. Writes the datagrams to `udp_out`.
/// Returns if the TCP socket is closed, or an IO error happens on either socket.
pub async fn process_tcp2udp(
    mut tcp_in: TcpReadHalf,
    sender: &mpsc::Sender<Vec<u8>>,
    tcp_recv_timeout: Option<Duration>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = datagram_buffer();
    // `buffer` has unprocessed data from the TCP socket up until this index.
    let mut unprocessed_i = 0;
    loop {
        let tcp_read_len =
            maybe_timeout(tcp_recv_timeout, tcp_in.read(&mut buffer[unprocessed_i..]))
                .await
                .context("Timeout while reading from TCP")?
                .context("Failed reading from TCP")?;
        if tcp_read_len == 0 {
            break;
        }
        unprocessed_i += tcp_read_len;

        let processed_i = forward_datagrams_in_buffer(sender, &buffer[..unprocessed_i])
            .await
            .context("Failed connecting to Channel")
            .unwrap();

        // If we have read data that was not forwarded, because it was not a complete datagram,
        // move it to the start of the buffer and start over
        if unprocessed_i > processed_i {
            buffer.copy_within(processed_i..unprocessed_i, 0);
        }
        unprocessed_i -= processed_i;
    }
    log::debug!("TCP socket closed");
    Ok(())
}

async fn maybe_timeout<F: Future>(
    duration: Option<Duration>,
    future: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    match duration {
        Some(duration) => timeout(duration, future).await,
        None => Ok(future.await),
    }
}

/// Forward all complete datagrams in `buffer` to `udp_out`.
/// Returns the number of processed bytes.
async fn forward_datagrams_in_buffer(
    sender: &mpsc::Sender<Vec<u8>>,
    buffer: &[u8],
) -> Result<usize, TrySendError<()>> {
    let mut unprocessed_buffer = buffer;
    loop {
        let Some((datagram_data, tail)) = split_first_datagram(unprocessed_buffer) else {
            // The buffer does not contain the entire datagram
            break Ok(buffer.len() - unprocessed_buffer.len());
        };

        sender.try_send(datagram_data.to_vec()).map_or_else(
            |e| match e {
                TrySendError::Full(_) => Ok(()),
                TrySendError::Closed(_) => Err(TrySendError::Closed(())),
            },
            |_| Ok(()),
        )?;
        log::trace!("Forwarded {} bytes TCP->Channel", datagram_data.len());

        unprocessed_buffer = tail;
    }
}

/// Parses the header at the beginning of the `buffer` and if it contains a full
/// `udp-to-tcp` datagram it splits the buffer and returns the datagram data and
/// buffer tail as two separate slices: `(datagram_data, tail)`
fn split_first_datagram(buffer: &[u8]) -> Option<(&[u8], &[u8])> {
    let (header, tail) = buffer.split_first_chunk::<HEADER_LEN>()?;
    let datagram_len = usize::from(u16::from_be_bytes(*header));

    tail.split_at_checked(datagram_len)
}

/// Reads datagrams from `udp_in` and writes them (with the 16 bit header containing the length)
/// to `tcp_out` indefinitely, or until an IO error happens on either socket.
pub async fn process_udp2tcp<T: TcpPool>(
    udp_in: &UdpSocket,
    tcp_pool: &T,
) -> Result<Infallible, Box<dyn std::error::Error>> {
    // A buffer large enough to hold any possible UDP datagram plus its 16 bit length header.
    let mut buffer = datagram_buffer();
    loop {
        let udp_read_len = udp_in
            .recv(&mut buffer[HEADER_LEN..])
            .await
            .context("Failed reading from UDP")?;

        // Set the "header" to the length of the datagram.
        let datagram_len =
            u16::try_from(udp_read_len).expect("UDP datagram can't be larger than 2^16");
        buffer[..HEADER_LEN].copy_from_slice(&datagram_len.to_be_bytes()[..]);

        tcp_pool
            .write_all(buffer[..HEADER_LEN + udp_read_len].to_vec())
            .await
            .context("Failed writing to TCP")?;

        log::trace!("Forwarded {} bytes UDP->TCP", udp_read_len);
    }
}

/// Creates and returns a buffer on the heap with enough space to contain any possible
/// UDP datagram.
///
/// This is put on the heap and in a separate function to avoid the 64k buffer from ending
/// up on the stack and blowing up the size of the futures using it.
#[inline(never)]
pub fn datagram_buffer() -> Box<[u8; MAX_DATAGRAM_SIZE]> {
    Box::new([0u8; MAX_DATAGRAM_SIZE])
}
