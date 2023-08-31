pub const INITIALIZATION:u8 = 0;
pub const ELECTION:u8 = 1;
pub const CONFIGURATION:u8 = 2;
pub const RECOVERY:u8 = 3;
pub const NORMAL:u8 = 4;

pub const LEADER_VOTE_TIMEOUT:u64 = 2500;
pub const LEADERSHIP_VOTE_TIMEOUT:u64 = 3000;
pub const LEADER_LEASE_TIMEOUT:u64 = 10000;