use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub enum PaxosMessage {
    Propose{ value: String},
    Promise { proposal_number: u32, prev_proposal_number: u32, prev_value: Option<String> },
    Accept { proposal_number: u32, proposal_value: Option<String> },
    Accepted { proposal_number: u32, value: Option<String> },
}