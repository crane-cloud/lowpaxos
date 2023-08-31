use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use crate::role::Role;
//use crate::monitor::ProfileMatrix;
use crate::constants::INITIALIZATION;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Replica {
    pub id: u32,
    pub replica_address: SocketAddr,
    pub status: u8,
    pub role: Role,
    pub profile: Option<f32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub propose_term: u32,
    pub n: usize,
    pub f: usize,
    pub replicas: Vec<Replica>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub propose_term: u32,
    pub replica: Replica,
}

impl Replica {
    pub fn new(id: u32, replica_address: SocketAddr) -> Self { 
        Replica {
            id,
            replica_address,
            status: INITIALIZATION,
            role: Role::new(),
            profile: None,
        }
    }

}

impl Config {
    pub fn new(propose_term: u32, n: usize, f: usize, replicas: Vec<Replica>) -> Self {
        Config {
            propose_term,
            n,
            f,
            replicas,
        }
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
}

impl ReplicaConfig {
    pub fn new(propose_term: u32, replica: Replica) -> Self {
        ReplicaConfig {
            propose_term,
            replica,
        }
    }
}