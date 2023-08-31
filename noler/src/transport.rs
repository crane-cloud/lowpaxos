use std::net::{SocketAddr, UdpSocket};
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
}