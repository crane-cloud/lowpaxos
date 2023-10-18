use crate::noler_msg_checker::NolerMsg;

use serde::{Serialize, Deserialize};
use crate::election_actor::KvStoreMsg;

use stateright::actor::Id;

//SR Log
type Ballot = (u32, u64);
type RequestId = u64;
type Key = u64;
type Value = u64;
type Request = (RequestId, Id, Key, Option<Value>);

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
    pub reply: Option<KvStoreMsg<RequestId, Key, Value, NolerMsg>>,
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
            start: 1,
        }
    }

    // Append a new log entry
    pub fn append(&mut self, ballot: Ballot, request: Request, reply: Option<KvStoreMsg<u64, u64, u64,  NolerMsg>>, state: LogEntryState) -> &mut LogEntrySR {

        let new_entry = LogEntrySR {
            ballot,
            state,
            request,
            reply,
        };

        self.entries.push(new_entry);
        self.entries.last_mut().unwrap()
    }

    //Find a log entry by operation number
    pub fn find(&self, opnum: u64) -> Option<&LogEntrySR> {
        //println!("viewstamp (view-last_op): <{} - {}> ; opnum: {}", entry.viewstamp.0, entry.viewstamp.1, opnum);
        if let Some(entry) = self.entries.get((opnum - self.start) as usize) {
            if entry.ballot.1 == opnum {
                println!("An entry with opnum {} was found, has the same ballot.last_op {}", opnum, entry.ballot.1);
                Some(entry)
            } else {
                println!("An entry with opnum {} was found, but it has a different ballot.last_op {} ", opnum, entry.ballot.1);
                None
            }
        } else {
            println!("No entry with opnum {} was found", opnum);
            None
        }
    }
    
        // Set status of a log entry
        pub fn set_status(&mut self, opnum: u64, state: LogEntryState) -> bool {
            if let Some(entry) = self.find_mut(opnum) {
                entry.state = state;
                true
            } else {
                false
            }
        }
    
    
        //Set the response message of a log entry
        pub fn set_reply(&mut self, opnum:u64, reply: KvStoreMsg<u64, u64, u64, NolerMsg>) -> bool {
            if let Some(entry) = self.find_mut(opnum) {
                entry.reply = Some(reply);
                true
            } else {
                println!("No entry found");
                false
            }
        }
    
        // Find a mutable log entry by operation number and update status
        pub fn find_mut(&mut self, opnum: u64) -> Option<&mut LogEntrySR> {
            if let Some(entry) = self.entries.get_mut((opnum - self.start) as usize) {
                if entry.ballot.1 == opnum {
                    Some(entry)
                } else {
                    None
                }
            } else {
                None
            }
        }
    
        //Return log entries from one operation number to another
        pub fn get_entries(&self, start: u64, end: u64) -> Vec<LogEntrySR> {
            let mut entries = Vec::new();
            for i in start..=end {
                if let Some(entry) = self.find(i) {
                    entries.push(entry.clone());
                }
            }
            entries
        }
    
        //Append log entries from one operation number to another
        pub fn append_entries(&mut self, entries: Vec<LogEntrySR>) {
            for entry in entries {
                self.entries.push(entry);
            }
        }







    // Find a log entry by ballot.propose_number
    pub fn _find(&self, ballot: Ballot) -> Option<&LogEntrySR> {
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

    //Find a log entry by ballot & request_id
    pub fn find_by_ballot_request_id(&self, ballot: Ballot, request_id: u64) -> Option<&LogEntrySR> {
        if let Some(entry) = self.entries.iter().find(|&entry| entry.ballot == ballot && entry.request.0 == request_id) {
            Some(entry)
        } else {
            None
        }
    }

    //Find a log entry by request_id
    pub fn get_by_request_id(&self, request_id: RequestId, key: u64) -> Option<&LogEntrySR> {
        if let Some(entry) = self.entries.iter().find(|&entry| entry.reply == Some(KvStoreMsg::SetOk(request_id, key))) {
            Some(entry)
        } else {
            None
        }
    }

    // Set status of a log entry
    pub fn _set_status(&mut self, ballot: Ballot, state: LogEntryState) -> bool {
        if let Some(entry) = self.find_mut(ballot.1) {
            entry.state = state;
            true
        } else {
            false
        }
    }

    //Set the reply of a log entry
    pub fn _set_reply(&mut self, ballot: Ballot, reply: KvStoreMsg<u64, u64, u64, NolerMsg>) -> bool {
        if let Some(entry) = self.find_mut(ballot.1) {
            entry.reply = Some(reply);
            true
        } else {
            false
        }
    }

    // Find a mutable log entry by propose number and update status
    pub fn _find_mut(&mut self, ballot: Ballot) -> Option<&mut LogEntrySR> {
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
    pub fn _last(&self) -> Option<&LogEntrySR> {
        self.entries.last()
    }
    
    // Get the last ballot in the log
    pub fn _last_ballot(&self) -> (u32, u64) {
        if self.entries.is_empty() {
            (0, self.start - 1)
        } else {
            self.entries.last().unwrap().ballot
        }
    }
    
    // Get the first ballot number in the log
    pub fn _first_ballot(&self) -> u64 {
        self.start
    }
    
    // Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
}