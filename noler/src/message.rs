use std::net::SocketAddr;
use rand_distr::num_traits::ops::bytes;
use serde::{Serialize, Deserialize};

use crate::role::Role;
use crate::config::Config;
use crate::log::{Log, LogEntry};
use crate::monitor::{Profile};


#[derive(Debug)]
pub struct ChannelMessage {
    pub channel: String,
    pub src: SocketAddr,
    pub message: String,
}

#[derive(Debug)]
pub struct ClientMessage {
    pub msg_type: MessageType,
    //pub message: Vec<u8>
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone, Eq, Hash)]
pub enum ElectionType {
    Normal,
    Offline,
    Profile,
    Timeout,
    Degraded,
}

impl Default for ElectionType {
    fn default() -> Self {
        ElectionType::Normal
    }
}

pub enum TimeoutType {
    LeaderInitTimeout(String),
    LeaderVoteTimeout(String),
    LeadershipVoteTimeout(String),
    LeaderLeaseTimeout(String),
}

#[derive(Debug)]
pub enum MessageType {
    RequestVote,
    ResponseVote,
    RequestReplicaConfig,
    ConfigureReplica,
    HeartBeat,
    PollLeader,
    PollLeaderOk,
    Request,
    Response,
    Propose,
    ProposeOk,
    Commit,
    RequestConfig,
    ChangeConfig,
    RequestState,
    UpdateState,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct RequestVoteMessage {
    pub replica_id: u32,
    pub replica_address: SocketAddr,
    pub replica_role: Role,
    pub ballot: (u32, u64),
    pub replica_profile: u8,
    pub election_type: ElectionType,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, Hash, PartialEq)]
pub struct ResponseVoteMessage {
    pub replica_id: u32,
    pub replica_address: SocketAddr,
    pub ballot: (u32, u64),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartBeatMessage {
    pub leader_id: u32,
    pub leader_address: SocketAddr,
    pub ballot: (u32, u64),
    pub replica_profile: u8,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigMessage {
    pub leader_id: u32,
    pub config: Config,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct RequestConfigMessage {
    pub replica_id: u32,
    pub replica_address: SocketAddr,
    pub ballot: (u32, u64),
}


#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct PollLeaderMessage {
    pub candidate_id: u32,
    pub candidate_address: SocketAddr,
    pub ballot: (u32, u64),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PollLeaderOkMessage {
    pub leader_id: u32,
    pub ballot: (u32, u64),
    pub replica_profile: u8,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessage {
    pub client_id: SocketAddr,
    pub request_id: u64,
    pub operation: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseMessage {
    pub request_id: u64,
    pub reply: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeMessage {
    pub ballot: (u32, u64),
    pub request: RequestMessage,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProposeOkMessage {
    pub ballot: (u32, u64),
    pub commit_index: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitMessage {
    pub ballot: (u32, u64),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestStateMessage {
    pub ballot: (u32, u64),
    pub commit_index: u64,
}

// #[allow(dead_code)]
// #[derive(Debug, Serialize, Deserialize)]
// pub struct ResponseStateMessage {
//     pub ballot: (u32, u64),
//     pub commit_index: u64,
//     pub log: Log,
// }

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct LogStateMessage {
    pub ballot: (u32, u64),
    pub commit_index: u64,
    pub log: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestProfileMessage {
    pub id: u32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseProfileMessage {
    pub profiles: Vec<Profile>,
}






#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestReplicaConfigMessage {
    replica_address: SocketAddr,
    propose_term: u32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigureReplicaMessage {
    pub leader_address: SocketAddr,
    pub propose_term: u32,
    pub replica_role: Role,
}


#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeConfigMessage {
    leader_address: SocketAddr,
    propose_term: u32,
    propose_number: u64,
    //commit_index: u64,
    config: Config,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateStateMessage {
    leader_address: SocketAddr,
    propose_term: u32,
    propose_number: u64,
    commit_index: u64,
    log: Log,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeReadMessage {
    candidate_address: SocketAddr,
    execute_index: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProposeReadOkMessage {
    candidate_address: SocketAddr,
    execute_index: u64,
}


#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageWrapper {
    pub msg_type: String,
    pub msg_content: String,
}