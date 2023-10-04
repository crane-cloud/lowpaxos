use crate::message::{RequestMessage, ResponseMessage};
use kvstore::KvStoreMsg;

use serde::{Serialize, Deserialize};

type Ballot =(u32, u64);
type Key = String;
type Value = String;

// Define the LogEntryState enum
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Copy, Clone)]
pub enum LogEntryState {
    Request,
    Propose,
    Proposed,
    Committed,
    Executed,
}

// Define the LogEntry struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    pub ballot: Ballot,
    pub state: LogEntryState,
    pub request: RequestMessage,
    pub response: Option<KvStoreMsg<Key, Value>>,
}

// Define the Log struct
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Log {
    entries: Vec<LogEntry>,
    start: u64,
}

impl Log {

    // Constructor
    pub fn new() -> Self {
        Log {
            entries: Vec::new(),
            start: 1,
        }
    }

    // Append a new log entry
    pub fn append(&mut self, ballot: Ballot, req: RequestMessage, resp: Option<KvStoreMsg<Key, Value>>, state: LogEntryState) -> &mut LogEntry {

        let new_entry = LogEntry {
            ballot,
            state,
            request: req,
            response: resp,
        };

        self.entries.push(new_entry);
        self.entries.last_mut().unwrap()
    }

    //Find a log entry by ballot & request_id
    pub fn find_by_ballot_request_id(&self, ballot: Ballot, request_id: u64) -> Option<&LogEntry> {
        if let Some(entry) = self.entries.iter().find(|entry| entry.ballot == ballot && entry.request.request_id == request_id) {
            Some(entry)
        } else {
            println!("No entry found");
            None
        }
    }

    // Find a log entry by ballot
    pub fn find_by_ballot(&self, ballot: Ballot) -> Option<&LogEntry> {
        if let Some(entry) = self.entries.iter().find(|entry| entry.ballot == ballot) {
            Some(entry)
        } else {
            println!("No entry found");
            None
        }
    }

    // Find a log entry by operation/propose number
    pub fn find(&self, opnum: u64) -> Option<&LogEntry> {
        //println!("viewstamp (view-last_op): <{} - {}> ; opnum: {}", entry.viewstamp.0, entry.viewstamp.1, opnum);
        if let Some(entry) = self.entries.get((opnum - self.start) as usize) {
            if entry.ballot.1 == opnum {
                println!("An entry with opnum {} was found, has the same viewstamp.last_op {}", opnum, entry.ballot.1);
                Some(entry)
            } else {
                println!("An entry with opnum {} was found, but it has a different viewstamp.last_op {} ", opnum, entry.ballot.1);
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
    pub fn set_response_message(&mut self, opnum:u64, response: KvStoreMsg<Key, Value>) -> bool {
        if let Some(entry) = self.find_mut(opnum) {
            entry.response = Some(response);
            true
        } else {
            println!("No entry found");
            false
        }
    }

    // Find a mutable log entry by operation number and update status
    pub fn find_mut(&mut self, opnum: u64) -> Option<&mut LogEntry> {
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
    pub fn get_entries(&self, start: u64, end: u64) -> Vec<&LogEntry> {
        let mut entries = Vec::new();
        for i in start..=end {
            if let Some(entry) = self.find(i) {
                entries.push(entry);
            }
        }
        entries
    }

    //Append log entries from one operation number to another
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        for entry in entries {
            self.entries.push(entry);
        }
    }

    //Find a mutable log entry by ballot and update status
    
    // Get the last log entry
    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.last()
    }
    
    // Get the last propose number in the log
    pub fn last_opnum(&self) -> u64 {
        if self.entries.is_empty() {
            self.start - 1
        } else {
            self.entries.last().unwrap().ballot.1
        }
    }
    
    // Get the first propose number in the log
    pub fn first_opnum(&self) -> u64 {
        self.start
    }
    
    // Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
}