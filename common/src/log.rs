use crate::message::{RequestMessage, ReplyMessage};
use sha2::{Sha256, Digest};

// Define the LogEntryState enum
#[derive(Debug, PartialEq)]
pub enum LogEntryState {
    Request,
    Prepare,
    Prepared,
    Committed,
}

// Define the LogEntry struct
pub struct LogEntry {
    viewstamp: (u64, u64),
    state: LogEntryState,
    request: RequestMessage,
    hash: String,
    prev_client_req_opnum: u64,
    reply_message: Option<ReplyMessage>,
}

// Define the Log struct
pub struct Log {
    entries: Vec<LogEntry>,
    initial_hash: String,
    start: u64,
    use_hash: bool,
}

impl Log {
    // Define constants
    const EMPTY_HASH: &'static str = "";

    // Constructor
    pub fn new(use_hash: bool, start: u64, initial_hash: &str) -> Self {
        Log {
            entries: Vec::new(),
            initial_hash: initial_hash.to_string(),
            start,
            use_hash,
        }
    }

    // Append a new log entry
    pub fn append(&mut self, mut viewstamp: (u64, u64), req: RequestMessage, state: LogEntryState) -> &mut LogEntry {
        //assert_eq!(self.entries.is_empty(), viewstamp.1 == self.start); //?

        // if self.entries.is_empty() {
        //     viewstamp.1 = self.start;
        // }

        let prev_hash = self.last_hash();
        let mut new_entry = LogEntry {
            viewstamp,
            state,
            request: req,
            hash: String::new(),
            prev_client_req_opnum: 0,
            reply_message: None,
        };

        if self.use_hash {
            new_entry.hash = Log::compute_hash(&prev_hash, &new_entry);
        }

        self.entries.push(new_entry);
        self.entries.last_mut().unwrap()
    }

    // Find a log entry by operation number
    pub fn find(&self, opnum: u64) -> Option<&LogEntry> {
        //println!("viewstamp (view-last_op): <{} - {}> ; opnum: {}", entry.viewstamp.0, entry.viewstamp.1, opnum);
        if let Some(entry) = self.entries.get((opnum - self.start) as usize) {
            if entry.viewstamp.1 == opnum {
                println!("An entry with opnum {} was found, has the same viewstamp.last_op {}", opnum, entry.viewstamp.1);
                Some(entry)
            } else {
                println!("An entry with opnum {} was found, but it has a different viewstamp.last_op {} ", opnum, entry.viewstamp.1);
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

    // Find a mutable log entry by operation number and update status
    pub fn find_mut(&mut self, opnum: u64) -> Option<&mut LogEntry> {
        if let Some(entry) = self.entries.get_mut((opnum - self.start) as usize) {
            if entry.viewstamp.1 == opnum {
                Some(entry)
            } else {
                None
            }
        } else {
            None
        }
    }

    // Compute hash for a log entry
    pub fn compute_hash(prev_hash: &str, entry: &LogEntry) -> String {
        let mut hasher = Sha256::new();
        hasher.update(prev_hash.as_bytes());
        // hasher.update(&entry.request.clientid().to_be_bytes());
        // hasher.update(&entry.request.clientreqid().to_be_bytes());

        // You can also hash other relevant fields of the entry here

        let hash_result = hasher.finalize();

        let hash_string = format!("{:x}", hash_result);
        hash_string
    }

    // Get the last hash in the log
    pub fn last_hash(&self) -> &str {
        if self.entries.is_empty() {
            &self.initial_hash
        } else {
            &self.entries.last().unwrap().hash
        }
    }

    // Remove entries after a given operation number
    pub fn remove_after(&mut self, opnum: u64) {
        // Ensure we're not removing committed entries
        for i in opnum..=self.last_opnum() {
            assert_ne!(self.find(i).unwrap().state, LogEntryState::Committed);
            //assert_ne!(self.find(i).unwrap().state, LogEntryState::Prepared);
        }
    
        if opnum > self.last_opnum() {
            return;
        }
    
        println!("Removing log entries after {}", opnum);
    
        self.entries.truncate((opnum - self.start + 1) as usize);
    }
    
    // Get the last log entry
    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.last()
    }
    
    // Get the last viewstamp in the log
    pub fn last_viewstamp(&self) -> (u64, u64) {
        if self.entries.is_empty() {
            (0, self.start - 1)
        } else {
            self.entries.last().unwrap().viewstamp
        }
    }
    
    // Get the last operation number in the log
    pub fn last_opnum(&self) -> u64 {
        self.last_viewstamp().1
    }
    
    // Get the first operation number in the log
    pub fn first_opnum(&self) -> u64 {
        self.start
    }
    
    // Check if the log is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
}