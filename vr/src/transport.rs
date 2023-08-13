use std::net::{SocketAddr, UdpSocket};
use crate::configuration::Config;

pub struct Transport {
    socket: UdpSocket,
}

impl Transport {
    pub fn new(bind_address: SocketAddr) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(bind_address)?;

        Ok(Transport { socket })
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
            self.send(&replica.to_socket_addr(), data)?;
        }

        Ok(())
    }
}