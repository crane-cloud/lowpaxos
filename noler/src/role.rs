use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Member,
    Witness,
    Candidate,
    Leader,
}

impl Role {
    pub fn new() -> Role {
        Role::Member
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Member
    }
}
