use serde::{Serialize, Deserialize};

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub enum ViewSrMessage {
    Request(Request),
    UnloggedRequest(UnloggedRequest),
    RequestMessage(RequestMessage),
    ReplyMessage(ReplyMessage),
    UnloggedRequestMessage(UnloggedRequestMessage),
    UnloggedReplyMessage(UnloggedReplyMessage),
    PrepareMessage(PrepareMessage),
    PrepareOkMessage(PrepareOkMessage),
    CommitMessage(CommitMessage),
    CommitOKMessage(CommitOKMessage),
    RequestStateTransferMessage(RequestStateTransferMessage),
    LogEntry(LogEntry),
    StateTransferMessage(StateTransferMessage),
    StartViewChangeMessage(StartViewChangeMessage),
    DoViewChangeMessage(DoViewChangeMessage),
    StartViewMessage(StartViewMessage),
    RecoveryMessage(RecoveryMessage),
    RecoveryResponseMessage(RecoveryResponseMessage),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Request {
    pub op: Vec<u8>,
    pub clientid: u64,
    pub clientreqid: u64,
}
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnloggedRequest {
    unloggedrequest: Request,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestMessage {
    pub request: Request,
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
pub struct UnloggedRequestMessage {
    request: UnloggedRequest,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnloggedReplyMessage {
    reply: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrepareMessage {
    pub view: u64,
    pub opnum: u64,
    pub batchstart: u64,
    pub requests: Vec<Request>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct PrepareOkMessage {
    pub view: u64,
    pub opnum: u64,
    pub replicaidx: u32,
}

impl PrepareOkMessage {
    pub fn default() -> Self {
        PrepareOkMessage {
            view: 0,
            opnum: 0,
            replicaidx: 0,
        }
    }
}


#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitMessage {
    pub view: u64,
    pub opnum: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitOKMessage {
    view: u64,
    opnum: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestStateTransferMessage {
    view: u64,
    opnum: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    view: u64,
    opnum: u64,
    request: Request,
    state: Option<u32>,
    hash: Option<Vec<u8>>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct StateTransferMessage {
    view: u64,
    opnum: u64,
    entries: Vec<LogEntry>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct StartViewChangeMessage {
    view: u64,
    replicaidx: u32,
    lastcommitted: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DoViewChangeMessage {
    view: u64,
    lastnormalview: u64,
    lastop: u64,
    lastcommitted: u64,
    entries: Vec<LogEntry>,
    replicaidx: u32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct StartViewMessage {
    view: u64,
    lastop: u64,
    lastcommitted: u64,
    entries: Vec<LogEntry>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RecoveryMessage {
    replicaidx: u32,
    nonce: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RecoveryResponseMessage {
    view: u64,
    nonce: u64,
    entries: Vec<LogEntry>,
    lastop: Option<u64>,
    lastcommitted: Option<u64>,
    replicaidx: u32,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageWrapper {
    pub msg_type: String,
    pub msg_content: String,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ViewStamp {
    pub view: u64,
    pub opnum: u64,
}