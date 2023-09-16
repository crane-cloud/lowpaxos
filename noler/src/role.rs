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