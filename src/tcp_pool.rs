use std::io;

pub trait TcpPool {
    async fn write_all(&self, buffer: Vec<u8>) -> Result<(), io::Error>;
}