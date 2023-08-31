use crate::message::{RequestMessage, ResponseMessage};
use serde::{Serialize, Deserialize};

// Define the LogEntryState enum
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
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
    propose_term: u32,
    propose_number: u64,
    state: LogEntryState,
    request: RequestMessage,
    response_message: Option<ResponseMessage>,
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
            start: 0,
        }
    }

    // Append a new log entry
    pub fn append(&mut self, mut propose_term:u32, mut propose_number: u64, req: RequestMessage, state: LogEntryState) -> &mut LogEntry {

        let mut new_entry = LogEntry {
            propose_term,
            propose_number,
            state,
            request: req,
            response_message: None,
        };

        self.entries.push(new_entry);
        self.entries.last_mut().unwrap()
    }

    // Find a log entry by propose number
    pub fn find(&self, propose_number: u64) -> Option<&LogEntry> {
        if let Some(entry) = self.entries.get((propose_number) as usize) {
            if entry.propose_number == propose_number {
                println!("An entry with propose_number {} found", propose_number);
                Some(entry)
            } else {
                println!("An entry with propose_number {} not found", propose_number);
                None
            }
        } else {
            println!("No entry found");
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

    // Find a mutable log entry by propose number and update status
    pub fn find_mut(&mut self, propose_number: u64) -> Option<&mut LogEntry> {
        if let Some(entry) = self.entries.get_mut((propose_number - self.start) as usize) {
            if entry.propose_number == propose_number {
                Some(entry)
            } else {
                None
            }
        } else {
            None
        }
    }
    
    // Get the last log entry
    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.last()
    }
    
    // Get the last propose number in the log
    pub fn last_propose_number(&self) -> u64 {
        if self.entries.is_empty() {
            self.start - 1
        } else {
            self.entries.last().unwrap().propose_number
        }
    }
    
    // Get the first propose number in the log
    pub fn first_propose_number(&self) -> u64 {
        self.start
    }
    
    // Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
}