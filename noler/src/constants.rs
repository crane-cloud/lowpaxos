pub const INITIALIZATION:u8 = 0;
pub const ELECTION:u8 = 1;
pub const CONFIGURATION:u8 = 2;
pub const RECOVERY:u8 = 3;
pub const NORMAL:u8 = 4;
pub const OFFLINE:u8 = 5;

pub const LEADER_VOTE_TIMEOUT:u64 = 4;
pub const LEADERSHIP_VOTE_TIMEOUT:u64 = 6;

pub const LEADER_LEASE_TIMEOUT:u64 = 6;

pub const POLL_LEADER_TIMEOUT:u64 = 5;
pub const HEARTBEAT_TIMEOUT:u64 = 8;

pub const POLL_LEADER_TIMER:u64 = 3;

pub const POLL_MONITOR_TIMEOUT:u64 = 5;

// leader_lease_timeout < heartbeat_timeout  
// poll_leader_timer < poll_leader_timeout
// poll_leader_timeout < leader_lease_timeout

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum Status {
    INITIALIZATION,
    ELECTION,
    CONFIGURATION,
    RECOVERY,
    NORMAL,
    OFFLINE,
}

impl Default for Status {
    fn default() -> Self {
        Status::INITIALIZATION
    }
}