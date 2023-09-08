use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use crate::role::Role;
use crate::config::Config;
use crate::log::Log;


#[derive(Debug)]
pub struct ChannelMessage {
    pub channel: String,
    pub message: String,
}


#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
pub enum ElectionType {
    Normal,
    Offline,
    Profile,
    Timeout,
    Degraded,
}

pub enum TimeoutType {
    LeaderInitTimeout(String),
    LeaderVoteTimeout(String),
    LeadershipVoteTimeout(String),
    LeaderLeaseTimeout(String),
}

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
    pub propose_term: u32,
    pub replica_profile: f32,
    pub election_type: ElectionType,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct ResponseVoteMessage {
    pub replica_id: u32,
    pub replica_address: SocketAddr,
    pub propose_term: u32,
    pub propose_number: u64,
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
pub struct HeartBeatMessage {
    pub leader_id: u32,
    pub leader_address: SocketAddr,
    pub propose_term: u32,
    pub replica_profile: f32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct PollLeaderMessage {
    pub candidate_id: u32,
    pub candidate_address: SocketAddr,
    pub propose_term: u32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PollLeaderOkMessage {
    pub leader_id: u32,
    pub propose_term: u32,
    pub replica_profile: f32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessage {
    client_id: SocketAddr,
    request_id: u64,
    operation: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseMessage {
    request_id: u64,
    reply: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProposeMessage {
    leader_address: SocketAddr,
    propose_term: u32,
    propose_number: u64,
    request: RequestMessage,

}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProposeOkMessage {
    replica_address: SocketAddr,
    propose_term: u32,
    propose_number: u64,
    commit_index: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitMessage {
    replica_address: SocketAddr,
    propose_term: u32,
    propose_number: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestConfigMessage {
    pub replica_id: u32,
    pub replica_address: SocketAddr,
    pub propose_term: u32,
    //propose_number: u64,
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
pub struct RequestStateMessage {
    replica_address: SocketAddr,
    propose_term: u32,
    propose_number: u64,
    commit_index: u64,
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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigMessage {
    pub leader_id: u32,
    pub config: Config,
}


#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageWrapper {
    pub msg_type: String,
    pub msg_content: String,
}