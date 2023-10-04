use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use crate::role::Role;
use crate::constants::INITIALIZATION;

use stateright::actor::Id;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct Replica {
    pub id: u32,
    pub replica_address: SocketAddr,
    pub status: u8,
    pub role: Role,
    pub profile: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct Config {
    //pub propose_term: u32,
    pub ballot: (u32, u64),
    pub n: usize,
    pub f: usize,
    pub replicas: Vec<Replica>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct LeaderConfig {
    pub leader: Replica,
    pub config: Config,
}

#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct ReplicaConfig {
    pub ballot: (u32, u64),
    pub replica: Replica,
}

impl Replica {
    pub fn new(id: u32, replica_address: SocketAddr) -> Self { 
        Replica {
            id,
            replica_address,
            status: INITIALIZATION,
            role: Role::new(),
            profile: 0,
        }
    }

}

impl Config {
    pub fn new(ballot: (u32, u64), n: usize, f: usize, replicas: Vec<Replica>) -> Config {
        Config {
            ballot,
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

    // Method to remove a replica
    pub fn remove_replica(&mut self, replica_id: u32) {
        if let Some(index) = self.replicas.iter().position(|r| r.id == replica_id) {
            self.replicas.remove(index);
            self.n -= 1;
            //self.f -= 1; System-wide setting
        }
    }

    // Method to add a replica
    pub fn add_replica(&mut self, replica: Replica) {
        self.replicas.push(replica);
        self.n += 1;
        //self.f += 1; System-wide set
    }

}

impl ReplicaConfig {
    pub fn new(ballot: (u32, u64), replica: Replica) -> Self {
        ReplicaConfig {
            ballot,
            replica,
        }
    }
}





//Stateright versions

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq, Copy)]
pub struct ReplicaSr {
    pub id: Id,
    pub status: u8,
    pub role: Role,
    pub profile: u8,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct ConfigSr {
    pub leader: Id,
    pub ballot: (u32, u64),
    pub n: usize,
    pub replicas: Vec<ReplicaSr>,
}

impl ReplicaSr {
    pub fn new(id: Id) -> Self { 
        ReplicaSr {
            id,
            status: INITIALIZATION,
            role: Role::new(),
            profile: 0,
        }
    }

}

impl ConfigSr {
    pub fn new(ballot: (u32, u64), n: usize, replicas: Vec<ReplicaSr>) -> ConfigSr {
        ConfigSr{
            leader: Id::default(),
            ballot,
            n,
            replicas,
        }
    }
}