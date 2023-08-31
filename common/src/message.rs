use serde::{Serialize, Deserialize};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestMessage {
    pub op: Vec<u8>,
    pub clientid: u64,
    pub clientreqid: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplyMessage {
    view: u64,
    opnum: u64,
    reply: Vec<u8>,
    clientreqid: u64,
}

impl ReplyMessage {
    pub fn default() -> Self {
        ReplyMessage {
            view: 0,
            opnum: 0,
            reply: Vec::new(),
            clientreqid: 0,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageWrapper {
    pub msg_type: String,
    pub msg_content: String,
}