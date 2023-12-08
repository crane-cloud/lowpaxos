use std::net::{SocketAddr, UdpSocket};
use tokio::io::{self};
use std::time::Duration;

use crate::config::Config;

#[derive(Debug)]
pub struct Transport {
    pub socket: UdpSocket,
}

impl Transport {
    pub fn new(bind_address: SocketAddr) -> Transport {
        let socket = UdpSocket::bind(bind_address).unwrap();

        Transport { socket }
    }

    pub fn send(&self, remote_address: &SocketAddr, data: &[u8]) -> Result<usize, std::io::Error> {
        self.socket.send_to(data, remote_address)
    }

    pub fn receive(&self, buffer: &mut [u8]) -> Result<usize, std::io::Error> {
        self.socket.recv(buffer)
    }

    pub fn receive_from(&self, buffer: &mut [u8]) -> Result<(usize, SocketAddr), std::io::Error> {
        self.socket.recv_from(buffer)
    }

    pub fn broadcast(&self, config:&Config, data: &[u8]) -> Result<(), std::io::Error> {
        for replica in config.replicas.iter() {
            self.send(&replica.replica_address, data)?;
        }

        Ok(())
    }

    pub fn local_address(&self) -> SocketAddr {
        self.socket.local_addr().unwrap()
    }

}

#[derive(Debug)]
pub struct TransportClient {
    socket: tokio::net::UdpSocket,
}

impl TransportClient {
    pub async fn new(bind_address: SocketAddr) -> io::Result<TransportClient> {
        let socket = tokio::net::UdpSocket::bind(bind_address).await?;
        Ok(TransportClient { socket })
    }

    pub async fn send(&self, remote_address: &SocketAddr, data: &[u8]) -> io::Result<usize> {
        self.socket.send_to(data, remote_address).await
    }

    pub async fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        self.socket.recv(buffer).await
    }

    pub async fn receive_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (size, addr) = self.socket.recv_from(buffer).await?;
        Ok((size, addr))
    }

    pub fn local_address(&self) -> SocketAddr {
        self.socket.local_addr().unwrap()
    }
}

#[derive(Debug)]
pub struct TransportTimed {
    pub socket: UdpSocket,
    pub read_timeout: Option<Duration>,  // Add read timeout
    pub write_timeout: Option<Duration>, // Add write timeout
}

impl TransportTimed {
    pub fn new(bind_address: SocketAddr, read_timeout: Option<Duration>, write_timeout: Option<Duration>) -> TransportTimed {
        println!("TransportTimed with read_timeout: {:?}ms and write_timeout: {:?}ms", read_timeout, write_timeout);
        let socket = UdpSocket::bind(bind_address).unwrap();

        TransportTimed {
            socket,
            read_timeout,
            write_timeout,
        }
    }

    pub fn send(&self, remote_address: &SocketAddr, data: &[u8]) -> Result<usize, std::io::Error> {
        // Set the write timeout if specified
        if let Some(timeout) = self.write_timeout {
            self.socket.set_write_timeout(Some(timeout)).expect("Failed to set write timeout");
        }

        self.socket.send_to(data, remote_address)
    }

    pub fn receive(&self, buffer: &mut [u8]) -> Result<usize, std::io::Error> {
        // Set the read timeout if specified
        if let Some(timeout) = self.read_timeout {
            self.socket.set_read_timeout(Some(timeout)).expect("Failed to set read timeout");
        }

        self.socket.recv(buffer)
    }

    pub fn receive_from(&self, buffer: &mut [u8]) -> Result<(usize, SocketAddr), std::io::Error> {
        // Set the read timeout if specified
        if let Some(timeout) = self.read_timeout {
            self.socket.set_read_timeout(Some(timeout)).expect("Failed to set read timeout");
        }

        self.socket.recv_from(buffer)
    }

    pub fn local_address(&self) -> SocketAddr {
        self.socket.local_addr().unwrap()
    }
}

