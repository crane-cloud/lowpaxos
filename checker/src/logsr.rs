use crate::noler_msg_checker::NolerMsg;

use serde::{Serialize, Deserialize};
use stateright::actor::register::RegisterMsg;

use stateright::actor::Id;

//SR Log
type Ballot = (u32, u64);
type RequestId = u64;
type Value = char;
type Request = (RequestId, Id, Option<Value>);

// Define the LogEntryState enum
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum LogEntryState {
    Request,
    Propose,
    Proposed,
    Committed,
    Executed,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct LogEntrySR {
    pub ballot: Ballot,
    pub state: LogEntryState,
    pub request: Request,
    pub reply: Option<RegisterMsg<RequestId, Value, NolerMsg>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub struct LogSR {
    entries: Vec<LogEntrySR>,
    start: u64,
}

impl LogSR {

    // Constructor
    pub fn new() -> Self {
        LogSR {
            entries: Vec::new(),
            start: 2,
        }
    }

    // Append a new log entry
    pub fn append(&mut self, ballot: Ballot, request: Request, reply: Option<RegisterMsg<u64, char, NolerMsg>>, state: LogEntryState) -> &mut LogEntrySR {

        let new_entry = LogEntrySR {
            ballot,
            state,
            request,
            reply,
        };

        self.entries.push(new_entry);
        self.entries.last_mut().unwrap()
    }

    // Find a log entry by ballot.propose_number
    pub fn find(&self, ballot: Ballot) -> Option<&LogEntrySR> {
        if let Some(entry) = self.entries.get((ballot.1 - self.start) as usize) { 
            if entry.ballot.1 == ballot.1 {
                log::info!("An entry with ballot {:?} found", ballot);
                Some(entry)
            } else {
                log::info!("An entry with ballot {:?} not found", ballot);
                None
            }
        } else {
           log::info!("No ballot entry found for ballot {:?}", ballot);
            None
        }
    }

    //Find a log entry by request
    pub fn find_by_request(&self, request: Request) -> Option<&LogEntrySR> {
        if let Some(entry) = self.entries.iter().find(|&entry| entry.request == request) {
            Some(entry)
        } else {
            None
        }
    }

    //Find a log entry by request_id
    pub fn get_by_request_id(&self, request_id: RequestId) -> Option<&LogEntrySR> {
        if let Some(entry) = self.entries.iter().find(|&entry| entry.reply == Some(RegisterMsg::PutOk(request_id))) {
            Some(entry)
        } else {
            None
        }
    }

    // Set status of a log entry
    pub fn set_status(&mut self, ballot: Ballot, state: LogEntryState) -> bool {
        if let Some(entry) = self.find_mut(ballot) {
            entry.state = state;
            true
        } else {
            false
        }
    }

    //Set the reply of a log entry
    pub fn set_reply(&mut self, ballot: Ballot, reply: RegisterMsg<u64, char, NolerMsg>) -> bool {
        if let Some(entry) = self.find_mut(ballot) {
            entry.reply = Some(reply);
            true
        } else {
            false
        }
    }

    // Find a mutable log entry by propose number and update status
    pub fn find_mut(&mut self, ballot: Ballot) -> Option<&mut LogEntrySR> {
        if let Some(entry) = self.entries.get_mut((ballot.1 - self.start) as usize) {
            if entry.ballot.1 == ballot.1 {
                Some(entry)
            } else {
                None
            }
        } else {
            None
        }
    }
    
    // Get the last log entry
    pub fn last(&self) -> Option<&LogEntrySR> {
        self.entries.last()
    }
    
    // Get the last ballot in the log
    pub fn last_ballot(&self) -> (u32, u64) {
        if self.entries.is_empty() {
            (0, self.start - 1)
        } else {
            self.entries.last().unwrap().ballot
        }
    }
    
    // Get the first ballot number in the log
    pub fn first_ballot(&self) -> u64 {
        self.start
    }
    
    // Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
}