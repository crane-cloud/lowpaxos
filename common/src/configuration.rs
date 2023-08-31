// use std::cell::RefCell;

use crate::types::{ReplicaIdx, ViewNumber};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ReplicaAddress {
    pub host: String,
    pub port: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Config {
    pub n: usize,
    pub f: usize,
    pub replicas: Vec<ReplicaAddress>,
}

impl Config {
    pub fn new(n: usize, f: usize, replicas: Vec<ReplicaAddress>) -> Self {
        Config { n, f, replicas }
    }

    pub fn quorum_size(&self) -> usize {
        self.f + 1
    }

    pub fn fast_quorum_size(&self) -> usize {
        self.f + (self.f + 1)/2 + 1
    }

    pub fn quorum(&self) -> usize {
        self.quorum_size()
    }

    pub fn get_leader(&self, view: ViewNumber) -> ReplicaIdx {
        let leader = view % self.n;
        leader
    }
}

impl ReplicaAddress {
    pub fn new(host: &str, port: &str) -> Self {
        ReplicaAddress {
            host: host.to_string(),
            port: port.to_string(),
        }
    }

    pub fn to_socket_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.host, self.port).parse().unwrap()
    }
}