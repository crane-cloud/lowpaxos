use std::net::SocketAddr;
use std::time::Instant;
use rand::Rng;
use serde_json;
use std::thread;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::quorum::QuorumSet;
use crate::timeout::Timeout;
use common::utility::wrap_and_serialize;
use kvstore::{KVStore, Operation};

use crate::constants::*;
use crate::config::{Config, Replica};
use crate::role::Role;
use crate::log::Log;
use crate::log::{LogEntryState, LogEntry};
use crate::message::{ElectionType,
                    *,
                    };
use crate::transport::Transport;
use crate::monitor::Profile;

type Ballot = (u32, u64);

pub struct NolerReplica {
    pub id: u32,
    pub replica_address: SocketAddr,
    pub role: Role,
    pub state: u8,
    pub ballot: Ballot,
    pub voted: Option<(u32, Ballot, ElectionType)>,
    pub leader: Option<(u32, Ballot)>, //To be part of the config
    last_op: u64,
    liveness: HashMap<u32, Instant>, //Liveness of replicas updated at ProposeOk
    leadership_quorum: QuorumSet<(u32, u64), ResponseVoteMessage>,
    propose_quorum: QuorumSet<(u32, u64), ProposeOkMessage>,
    _propose_read_quorum: QuorumSet<(u32, u64), ProposeReadOkMessage>,
    pub monitor: Vec<Profile>,
    monitor_address: SocketAddr,
    log: Log,
    pub config: Config,
    pub transport:  Arc<Transport>,
    data: KVStore,
    commit_index: u64,  
    execution_index: u64,

    requests: HashMap<SocketAddr, HashMap<u64, (u64, bool)>>,

    pub leader_init_timeout: Timeout,
    pub leader_vote_timeout: Timeout,
    pub leader_profile_vote_timeout: Timeout,
    pub leadership_vote_timeout: Timeout,
    pub leader_lease_timeout: Timeout,
    pub poll_leader_timeout: Timeout,
    pub heartbeat_timeout: Timeout,

    pub poll_leader_timer: Timeout,

    pub poll_monitor_timeout: Timeout,

    tx: Arc<Sender<ChannelMessage>>,
    rx: Arc<Receiver<ChannelMessage>>,

}

impl NolerReplica {
    pub fn new(id: u32, replica_address: SocketAddr, config: Config, monitor: Vec<Profile>, monitor_address: SocketAddr, transport: Transport) -> NolerReplica {

        let (tx, rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) = unbounded();

        let init_timer = rand::thread_rng().gen_range(1..5);

        let tx_leader_init = tx.clone();
        let tx_leader_vote = tx.clone();
        let tx_leader_profile_vote = tx.clone();
        let tx_leadership_vote = tx.clone();
        let tx_leader_lease = tx.clone();
        let tx_poll_leader = tx.clone();
        let tx_heartbeat = tx.clone();
        let tx_poll_monitor = tx.clone();

        let tx_poll_leader_timer = tx.clone();

        let leader_init_timeout = Timeout::new(init_timer, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderInitTimeout".to_string(),
            };
            if let Err(err) = tx_leader_init.send(msg) {
                eprintln!("Error sending Leader Init Message message: {}", err);
            }
        }));

        let leader_vote_timeout = Timeout::new(LEADER_VOTE_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderVoteTimeout".to_string(),
            };
            if let Err(err) = tx_leader_vote.send(msg) {
                eprintln!("Error sending Leader Vote Message message: {}", err);
            }
        }));

        let leader_profile_vote_timeout = Timeout::new(LEADER_PROFILE_VOTE_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderProfileVoteTimeout".to_string(),
            };
            if let Err(err) = tx_leader_profile_vote.send(msg) {
                eprintln!("Error sending Leader ProfileVote Message message: {}", err);
            }
        }));

        let leadership_vote_timeout = Timeout::new(LEADERSHIP_VOTE_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeadershipVoteTimeout".to_string(),
            };
            if let Err(err) = tx_leadership_vote.send(msg) {
                eprintln!("Error sending Leadership Vote Message message: {}", err);
            }
        }));

        let leader_lease_timeout = Timeout::new(LEADER_LEASE_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderLeaseTimeout".to_string(),
            };
            if let Err(err) = tx_leader_lease.send(msg) {
                eprintln!("Error sending Leader Lease Message message: {}", err);
            }

        }));

        let poll_leader_timeout = Timeout::new(POLL_LEADER_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "PollLeaderTimeout".to_string(),
            };
            if let Err(err) = tx_poll_leader.send(msg) {
                eprintln!("Error sending Poll Leader Message message: {}", err);
            }
        }));

        let heartbeat_timeout = Timeout::new(HEARTBEAT_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "HeartBeatTimeout".to_string(),
            };
            if let Err(err) = tx_heartbeat.send(msg) {
                eprintln!("Error sending Heartbeat Message message: {}", err);
            }
        }));

        let poll_leader_timer = Timeout::new(POLL_LEADER_TIMER, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "PollLeaderTimer".to_string(),
            };
            if let Err(err) = tx_poll_leader_timer.send(msg) {
                eprintln!("Error sending Poll Leader Timer Message message: {}", err);
            }
        }));

        let poll_monitor_timeout = Timeout::new(POLL_MONITOR_TIMEOUT, Box::new(move || {
            let msg = ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "PollMonitorTimeout".to_string(),
            };
            if let Err(err) = tx_poll_monitor.send(msg) {
                eprintln!("Error sending Poll Monitor Timer Message message: {}", err);
            }
        }));


        NolerReplica {
            id: id,
            replica_address: replica_address,
            role: Role::new(),
            state: INITIALIZATION,
            ballot: (0, 0),
            voted: None,
            leader: None,
            last_op: 0,
            liveness: HashMap::new(),
            leadership_quorum: QuorumSet::new(config.f as i32),
            propose_quorum: QuorumSet::new(config.f as i32),
            _propose_read_quorum: QuorumSet::new((config.f as i32)/2 + 1),
            monitor: monitor,
            monitor_address: monitor_address,
            log: Log::new(),
            config: config,
            transport: Arc::new(transport),
            data: KVStore::new(),
            commit_index: 0,
            execution_index: 0,

            requests: HashMap::new(),

            leader_init_timeout,
            leader_vote_timeout,
            leader_profile_vote_timeout,
            leadership_vote_timeout,
            leader_lease_timeout,
            poll_leader_timeout,
            heartbeat_timeout,

            poll_leader_timer,

            poll_monitor_timeout,

            tx: Arc::new(tx),
            rx: Arc::new(rx),

        }
    }

    // A replica starts an election cycle with an election type dependant on the timer received.
    fn start_election_cycle (&mut self, election_type: ElectionType) {

        //let ballot = (self.ballot.0 + 1, self.ballot.1 + 1);
        let ballot = (self.ballot.0 + 1, self.ballot.1);

        println!("{}: starting an election cycle with type {:?} and ballot {:?}", self.id, election_type, ballot);

        //Update the voted proprty
        self.voted = Some((self.id, ballot, election_type));

        assert!(self.role != Role::Leader, "Only a non-leader can start an election cycle");
        assert!(self.state == ELECTION, "Only a replica in election state can start an election cycle");

        if election_type == ElectionType::Offline {
            assert!(self.role == Role::Candidate || self.role == Role::Witness, "Only a candidate/witness can start offline election cycle");
            assert!(self.poll_leader_timeout.active() || self.heartbeat_timeout.active(), 
                "Only a candidate/witness with inactive poll leader timeout can start offline election cycle");
        }

        else if election_type == ElectionType::Profile {
            assert!(self.role == Role::Candidate || self.role == Role::Witness, "Only candidate/witness can start profile election cycle");
            //profile diff check
        }

        else if election_type == ElectionType::Timeout {
            assert!(self.role == Role::Candidate, "Only a candidate can start timeout election cycle");
            assert!(self.leader_vote_timeout.active(), "Only a candidate with inactive leader vote timeout can start timeout election cycle");
        }

        else if election_type == ElectionType::Degraded {
            assert!(self.role == Role::Member, "Only a member can start degraded election cycle");
            assert!(self.leadership_vote_timeout.active(), "Only a member with inactive leader vote timeout can start degraded election cycle");
        }

        else if election_type == ElectionType::Normal {
            assert!(self.role == Role::Member, "Only a member can start normal election cycle");
            assert!(self.leader_init_timeout.active(), "Only a member with inactive leader_init_timeout can start normal election cycle");
        }

        else {
            panic!("Invalid election type");
        }


        //Stop leader-election based timeouts
        self.leader_init_timeout.stop();
        self.leader_vote_timeout.stop();
        self.leadership_vote_timeout.stop();


        for replica in self.config.replicas.iter() {
                
            if replica.id != self.id {
                //Create a RequestVoteMessage with term + 1
                let request_vote_message = RequestVoteMessage {
                    replica_id: self.id,
                    replica_address: self.replica_address,
                    replica_role: self.role,
                    ballot: ballot,
                    //propose_term: self.propose_term + 1,
                    replica_profile: {
                        if let Some(profile) = self.monitor.get((replica.id - 1) as usize) { profile.get_x() }
                        else { 0 }
                    },
                    election_type: election_type,
                };

                // println!("{}: sending a RequestVoteMessage to Replica {}:{} with profile {}",
                //     self.id, 
                //     replica.id, 
                //     replica.replica_address,
                //     request_vote_message.replica_profile
                // );

                //Serialize the RequestVoteMessage with meta type set
                let serialized_rvm = wrap_and_serialize(
                    "RequestVoteMessage", 
                    serde_json::to_string(&request_vote_message).unwrap()
                );

                //Send the RequestVoteMessage to all replicas
                let _ = self.transport.send(
                    &replica.replica_address,
                    &mut serialized_rvm.as_bytes(),
                );
            }
        }
    }


    // Function to compare replica profiles
    fn profile_vote_result (&mut self, source_profile: u8, dest_profile: u8) -> bool {
        if source_profile >= dest_profile {
            return true;
        }
        else {
            return false;
        }
    }

    // Function to determine if received ballot is fresh
    fn fresh_ballot (&mut self, ballot: (u32, u64)) -> bool {
        //if ballot.0 > self.ballot.0 && ballot.1 > self.ballot.1 {
        if ballot.0 > self.ballot.0 && ballot.1 >= self.ballot.1 { // The term has to be different and the proposal must be >= local
            return true;
        }
        else {
            return false;
        }
    }

    //Function to determine if next ballot
    fn next_ballot (&mut self, ballot: (u32, u64)) -> bool { // Not relevant?
        //if ballot.0 == self.ballot.0 + 1 && ballot.1 == self.ballot.1 + 1 {
        if ballot.0 == self.ballot.0 + 1 && ballot.1 == self.ballot.1 { // Just new term but same op
            return true;
        }
        else {
            return false;
        }
    }

    //Function to determine if next proposal
    fn next_proposal(&mut self, ballot: (u32, u64)) -> bool {
        if ballot.0 >= self.ballot.0 && ballot.1 == self.ballot.1 + 1 {
            return true;
        }
        else {
            return false;
        }
    }

    //Function to determine if future ballot
    fn future_ballot(&mut self, ballot: (u32, u64)) -> bool {
        if ballot.0 > self.ballot.0 && ballot.1 > self.ballot.1 {
            return true;
        }

        else {
            return false;
        }
    }

    //Function to determine if future ballot
    fn stale_ballot(&mut self, ballot: (u32, u64)) -> bool {
        if ballot.0 < self.ballot.0 && ballot.1 < self.ballot.1 {
            return true;
        }

        else {
            return false;
        }
    }


    //Function to determine if stale ballot


    //Function to determine if voting is required or response should be provided immediately
    fn proceed_vote_else_response (&mut self, ballot: (u32, u64), message: RequestVoteMessage) {
        if self.next_ballot(ballot) {
            // Process the result normally
            self.process_profile_vote(message);

            return;
        }

        else {
            // Update the voted tuple
            //self.voted = (message.replica_id, Some(message.ballot));
            self.voted = Some((message.replica_id, message.ballot, message.election_type));

            // Provide a response immediately
            self.provide_vote_response(ballot, message.replica_address);

            return;
        }
    }

    // Function processes the profile in the RequestVoteMessage
    fn process_profile_vote (&mut self, message: RequestVoteMessage) {
        //Check if the replica's profile is better than the source's profile
        if self.profile_vote_result(
            message.replica_profile, {
            if let Some(profile) = self.monitor.get((message.replica_id - 1) as usize) { profile.get_x() }
            else { 0 }
        }, )
            {

            println!("{}: vote - Replica {}:{} has a better profile than Replica {}:{}", self.id,
                message.replica_id, 
                message.replica_profile,
                self.id,
                {
                    if let Some(profile) = self.monitor.get((message.replica_id - 1) as usize) { profile.get_x() }
                    else { 0 }
                },
                );

            //Update the voted tuple
            self.voted = Some((message.replica_id, message.ballot, message.election_type));               

            //Send the ResponseVoteMessage to the source
            self.provide_vote_response(message.ballot, message.replica_address);

            //Start the leadershipvotetimer
            let _ = self.leadership_vote_timeout.start();

            return;
            
        }
        else {
            println!("{}: noVote - Replica {}:{} has a better profile than Replica {}:{}", self.id, 
                self.id,
                {
                    if let Some(profile) = self.monitor.get((message.replica_id - 1) as usize) { profile.get_x() }
                    else { 0 }
                }, 
                message.replica_id,
                message.replica_profile,
                );

            println!("{}: changing the role to Candidate", self.id);
            self.role = Role::Candidate;

            //Start the leader_vote_timeout
            let _ = self.leader_vote_timeout.start();

            return;
        }
    }

    fn provide_vote_response (&mut self, ballot: (u32, u64), destination: SocketAddr) {
        //Create the ResonseVoteMessage
        let response_vote_message = ResponseVoteMessage {
            replica_id: self.id,
            replica_address: self.replica_address,
            ballot: ballot,
        };

        //Serialize the ResponseVoteMessage with meta type set
        let serialized_rsvm = wrap_and_serialize(
            "ResponseVoteMessage", 
            serde_json::to_string(&response_vote_message).unwrap()
        );

        //Send the ResponseVoteMessage to the source
        let k = self.transport.send(
            &destination,
            &mut serialized_rsvm.as_bytes(),
        );

        println!("{}: Sent the ResponseVoteMessage to Replica {} with status {:?}", self.id, destination, k);
        //return;
    }

    fn handle_request_vote_message (&mut self, src: SocketAddr, message: RequestVoteMessage) {

        println!("{}: received a RequestVoteMessage from Replica {} with my profile {}", self.id, message.replica_id, message.replica_profile);

        //Ignore if the replica is the source of the message
        if self.id == message.replica_id {
            println!("{}: received a RequestVoteMessage from itself!!!!!!", self.id);
            return;
        }

        ///////////////////////////////////////////////////////////////////////////
        //*First-time election request */
        if self.voted.is_none() && self.leader.is_none() {
            //Stop the leader init timeout
            self.leader_init_timeout.stop();

            //Check if the request has a fresh ballot
            if self.fresh_ballot(message.ballot) {
                    
                //Update the state to election
                self.state = ELECTION;
    
                //Process the vote request
                self.proceed_vote_else_response(message.ballot, message);

                return;
            }

            else {
                //Ignore the message
                println!("{}: stale ballot - ignoring vote request", self.id);
                return;
            }
        }

        ///////////////////////////////////////////////////////////////////////////
        //*Replica has voted before - but has no leader information */
        else if self.voted.is_some() && self.leader.is_none() {
            if message.ballot == self.voted.unwrap().1 && message.replica_id == self.voted.unwrap().0 && message.election_type == self.voted.unwrap().2 {
                //Provide the same response
                println!("{}: duplicate vote - resending same vote response", self.id);
                self.provide_vote_response(message.ballot, message.replica_address);
                return;
            }

            if message.ballot < self.voted.unwrap().1 {
                //Ignore the message
                println!("{}: stale ballot - ignoring vote request", self.id);
                return;
            }

            if message.ballot == self.voted.unwrap().1 {
                match message.election_type {
                    ElectionType::Timeout => if self.voted.unwrap().2 == ElectionType::Normal || self.voted.unwrap().2 == ElectionType::Degraded {
                        match self.role {
                            Role::Member => {
                                //Process the vote immediately 
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                self.provide_vote_response(message.ballot, message.replica_address);
                            }

                            Role::Candidate => {
                                //Process the request normally
                                self.proceed_vote_else_response(message.ballot, message);
                            }
                            _ => {
                                log::info!("{}: only member and candidate can vote", self.id);
                                return;
                            }
                        }
                    },

                    ElectionType::Degraded => if self.voted.unwrap().2 == ElectionType::Normal || self.voted.unwrap().2 == ElectionType::Timeout {
                        //Process the request immediately
                        self.voted = Some((message.replica_id, message.ballot, message.election_type));
                        self.provide_vote_response(message.ballot, message.replica_address);
                    },

                    _ => {
                        log::info!("{}: only timeout and degraded election types can be processed", self.id);
                        return;
                    }
                }
            }

            else if message.ballot > self.voted.unwrap().1 {
                //Process the vote immediately
                log::info!("{}: vote as request ballot > seen before and there is no leader", self.id);
                self.voted = Some((message.replica_id, message.ballot, message.election_type));
            }

        }

        ///////////////////////////////////////////////////////////////////////////
        //*Replica has never voted before - but has leader information */
        else if self.voted.is_none() && self.leader.is_some() {
            log::info!("{}: has a leader with ballot {:?}", self.id, self.ballot);

            //Check if the leader ballot is higher than the request ballot
            if self.leader.unwrap().1 >= message.ballot || self.ballot >= message.ballot {
                //Ignore the message
                log::info!("{}: has a higher leader/own ballot", self.id);
                return;
            }

            else {
                match message.election_type {
                    ElectionType::Profile => if self.role != Role::Leader {
                        match self.role {
                            Role::Candidate => {
                                if self.next_ballot(message.ballot) || self.next_ballot(self.leader.unwrap().1) {
                                    //Process the request normally
                                    self.proceed_vote_else_response(message.ballot, message);
                                }
                                else {
                                    //Provide a response immediately
                                    self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                    self.provide_vote_response(message.ballot, message.replica_address);
                                }
                            }

                            Role::Witness => {
                                //Vote immediately
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                self.provide_vote_response(message.ballot, message.replica_address);
                            }

                            _ => {
                                log::info!("{}: only candidate and witness can vote", self.id);
                                return;
                            }
                        }
                    }

                    ElectionType::Offline => if self.role != Role::Leader {
                        match self.role {
                            Role::Candidate => {
                                if self.next_ballot(message.ballot) || self.next_ballot(self.leader.unwrap().1) {
                                    //Process the request normally
                                    self.proceed_vote_else_response(message.ballot, message);
                                }
                                else {
                                    //Provide a response immediately
                                    self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                    self.provide_vote_response(message.ballot, message.replica_address);
                                }
                            }

                            Role::Witness => {
                                //Vote immediately
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                self.provide_vote_response(message.ballot, message.replica_address);
                            }

                            _ => {
                                log::info!("{}: only candidate and witness can vote", self.id);
                                return;
                            }
                        }
                    }

                    _ => {
                        log::info!("{}: only profile and offline election types can be processed", self.id);
                        return;
                    }
                }
            }
        }

        ///////////////////////////////////////////////////////////////////////////
        //*Replica has voted & leader information*//
        else if self.voted.is_some() && self.leader.is_some() {
            if message.ballot <= self.voted.unwrap().1 || message.ballot <= self.leader.unwrap().1 || message.ballot <= self.ballot {
                log::info!("{}: has a higher leader/voted ballot", self.id);
                return;
            }

            //Check if the ballot is the same as voted ballot
            if message.ballot == self.voted.unwrap().1 && message.replica_id == self.voted.unwrap().0 && message.election_type == self.voted.unwrap().2{
                //Provide the same response
                log::info!("{}: has a duplicate vote", self.id);
                self.provide_vote_response(message.ballot, message.replica_address);
                return;
            }

            match message.election_type {
                ElectionType::Normal => {
                    log::info!("{}: normal election request not allowed", self.id);
                    return;
                }

                ElectionType::Profile => if !self.stale_ballot(message.ballot){
                    match self.role {
                        Role::Leader => {
                            if self.profile_vote_result(
                                {
                                    if let Some(profile) = self.monitor.get((message.replica_id - 1) as usize) { profile.get_x() }
                                    else { 0 }
                                },
                                message.replica_profile, 
                            ){
                                // Leader profile is still better than source profile
                                println!("{}: leader profile still good - affirming leadership", self.id);

                                self.affirm_leadership(self.ballot);
                                
                                return;
                            }

                            else {

                                // Leader profile has degraded - no need to lead anymore
                                // Change role to candidate & status to election
                                println!("{}: leader profile detected low - changing the role to Candidate", self.id);

                                //self.role = Role::Candidate; //wait for the new role as provided by the leader
                                self.state = ELECTION;

                                if message.ballot.0 > self.ballot.0 && message.ballot.1 >= self.ballot.1 {
                                    //Update the voted tuple
                                    self.voted = Some((message.replica_id, message.ballot, message.election_type));

                                    //Send the ResponseVoteMessage to the source
                                    self.provide_vote_response(message.ballot, message.replica_address);

                                    //Start the leadervotetimer
                                    let _ = self.leader_profile_vote_timeout.start(); //Old leader can reclaim leadership at the end of this timeout

                                    return;

                                }

                                else {
                                    //Provide state to this replica so that it can ask to lead again with profile type of election
                                    //Retrieve the log from message.commit_index to our current commit_index

                                    println!("{}: retrieving log entries from {} to {} for {:?}", self.id, message.ballot.1, self.commit_index, src);
                                    let log = self.log.get_entries(message.ballot.1, self.commit_index);

                                    let len = log.len();

                                    //Serialize the log
                                    let log = serde_json::to_vec(&log).unwrap();

                                    //Send the state to the replica
                                    let state_message = LogStateMessage {
                                        ballot: self.ballot,
                                        commit_index: self.commit_index,
                                        log: log,
                                        profile: true,
                                    };

                                    //Serialize the LogStateMessage with meta type set
                                    let serialized_sm = wrap_and_serialize(
                                        "LogStateMessage", 
                                        serde_json::to_string(&state_message).unwrap()
                                    );

                                    //Send the LogStateMessage to the replica
                                    println!("{}: sending a LogStateMessage to Replica {} with ballot {:?} and length {}", self.id, src, message.ballot, len);
                                    let _ = self.transport.send(
                                        &src,
                                        &mut serialized_sm.as_bytes(),
                                    );
                                }

                                
                            }
                        }

                        Role::Candidate => {

                            if self.future_ballot(message.ballot) {
                                //Provide a response immediately
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                self.provide_vote_response(message.ballot, message.replica_address);

                                return;
                            }

                            if self.next_ballot(message.ballot) {
                                //Process the request normally
                                self.proceed_vote_else_response(message.ballot, message);

                                return;
                            }

                        }

                        Role::Witness => {
                            if self.future_ballot(message.ballot) {
                                //Provide a response immediately
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                self.provide_vote_response(message.ballot, message.replica_address);

                                return;
                            }

                            if self.next_ballot(message.ballot) {
                                //Process the request normally
                                self.proceed_vote_else_response(message.ballot, message);

                                return;
                            }
                        }

                        _ => {
                            log::info!("{}: only leader, candidate and witness can vote", self.id);
                            return;
                        }
                    }
                }

                ElectionType::Offline => {
                    match self.role {
                        Role::Leader => {
                            //Affirm leadership
                            println!("{}: received offline election - affirming leadership", self.id);


                            self.affirm_leadership(self.ballot);
                        }

                        Role::Candidate => {
                            if self.next_ballot(message.ballot) {
                                //Process the request normally
                                self.proceed_vote_else_response(message.ballot, message);
                            }
                            else {
                                //Provide a response immediately
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));
                                self.provide_vote_response(message.ballot, message.replica_address);
                            }
                        }

                        Role::Witness => {
                            //Vote immediately
                            self.voted = Some((message.replica_id, message.ballot, message.election_type));
                            self.provide_vote_response(message.ballot, message.replica_address);
                        }

                        _ => {
                            log::info!("{}: only leader, candidate and witness can vote", self.id);
                            return;
                        }
                    }
                }

                ElectionType::Timeout => if self.role == Role::Candidate || self.role == Role::Witness {
                    //Vote immediately

                    self.voted = Some((message.replica_id, message.ballot, message.election_type));
                    self.provide_vote_response(message.ballot, message.replica_address);
                }

                ElectionType::Degraded => if self.role == Role::Member || self.role == Role::Candidate {
                    //Vote immediately

                    self.voted = Some((message.replica_id, message.ballot, message.election_type));
                    self.provide_vote_response(message.ballot, message.replica_address);
                }
            }
            
        }
        ///////////////////////////////////////////////////////////////////////////
        else {
            log::info!("{}: invalid state", self.id);
            return;
        }
        ///////////////////////////////////////////////////////////////////////////

    }

    fn handle_response_vote_message (&mut self, message: ResponseVoteMessage) {

        //assert!(self.ballot == ((message.ballot.0 - 1), (message.ballot.1 - 1)), "Only a replica with the same ballot can handle a ResponseVoteMessage");
    
        println!("{}: received a ResponseVoteMessage from Replica {}", self.id, message.replica_id);

        if self.replica_address == message.replica_address {
            println!("!{}: received a ResponseVoteMessage from itself", self.id);
            return;
        }

        if self.state != ELECTION {
            println!("!{}: received a ResponseVoteMessage in state {}", self.id, self.state);
            return;
        }

        if self.ballot != ((message.ballot.0 - 1), (message.ballot.1)) {
            println!("!{}: received a ResponseVoteMessage from {} with stale term", self.id, message.replica_id);
            return;
        }

        if self.role == Role::Leader || self.role == Role::Witness {
            println!("!{}: received a ResponseVoteMessage in leader state", self.id);
            return;
        }

        if let Some(message) = self.leadership_quorum.add_and_check_for_quorum(message.ballot, message.replica_address, message.clone()) {
            //We have quorum for leadership - need to communicate to other replicas

            println!("{}: Quorum check -> {:?}", self.id, message);

            println!("{}: has quorum for leadership", self.id);

            //Stop all leader-election based timeouts
            self.leader_init_timeout.stop();
            self.leader_vote_timeout.stop();
            self.leadership_vote_timeout.stop();
            self.poll_leader_timeout.stop();
            self.heartbeat_timeout.stop();

            //Update the ballot to +1
            //self.ballot = (self.ballot.0 + 1, self.ballot.1 + 1);
            self.ballot = (self.ballot.0 + 1, self.ballot.1);

            //Update the leader tuple
            self.leader = Some((self.id, self.ballot));

            //Change the role to leader
            println!("{}: changing the role to Leader", self.id);
            self.role = Role::Leader;

            //Start the LeaderLeaseTimer
            let _ = self.leader_lease_timeout.start();
            
            //Change the state to normal
            self.state = NORMAL;

            //Set the config
            self.config = self.update_config();

            //Create the ConfigMessage
            let config_message = ConfigMessage {
                leader_id: self.id,
                config: self.config.clone(),
            };

            //Serialize the ConfigMessage with meta type set
            let serialized_cm = wrap_and_serialize(
                "ConfigMessage", 
                serde_json::to_string(&config_message).unwrap()
            );

            //Send the ConfigMessage to all replicas
            println!("{}: broadcasting the ConfigMessage to all replicas", self.id);
            let _ = self.transport.broadcast(
                &self.config,
                &mut serialized_cm.as_bytes(),
            );

            //Clear the liveness map
            self.liveness.clear();

            //Update the liveness map
            for replica in self.config.replicas.iter() {
                if replica.id != self.id {
                    self.liveness.insert(replica.id, Instant::now());
                }
            }
            
            return;
        }

        else {
            println!("{}: does not have quorum for leadership", self.id);
            //return;
        }

    }

    fn update_config(&mut self) -> Config {
        //Add the profiles to the config

        let mut config = self.config.clone();

        let role_mapping = vec![
            (0, 0, Role::Leader),
            (1, ((config.n - 1)/2), Role::Candidate),
            ((config.n - 1)/2 + 1, (config.n - 1), Role::Witness),
        ];

        for replica in config.replicas.iter_mut() {
            if replica.id == self.id {
                replica.profile = 100;
            }

            else {
                replica.profile = {
                    if let Some(profile) = self.monitor.get((replica.id - 1) as usize) { profile.get_x() }
                    else { 0 }
                };
            }
        }

        //Sort the config by profile
        config.replicas.sort_by(|a, b| b.profile.partial_cmp(&a.profile).unwrap());

        //Set the roles
        for (x, y, z) in role_mapping {
            for i in x..=y {
                config.replicas[i as usize].role = z;
            }
        }

        //Set the propose_term
        config.ballot = self.ballot; //ToDo - Not sure if best to do it here

        config

    }

    fn send_state_request(&mut self, leader_address: SocketAddr) {
        //Change state to recovery
        println!("{}: --> changing state to recovery", self.id);
        self.state = RECOVERY;

        // Create RequestStateMessage
        let request_state_message = RequestStateMessage {
            ballot: self.ballot,
            commit_index: self.commit_index,
        };
    
        // Serialize the RequestStateMessage
        let serialized_rsm = wrap_and_serialize(
            "RequestStateMessage",
            serde_json::to_string(&request_state_message).expect("Failed to serialize RequestStateMessage"),
        );
    
        // Print debug information
        println!("{}: sending a RequestStateMessage to leader {:?} with my ballot {:?}", self.id, self.leader.unwrap().0, self.ballot);
    
        // Send the RequestStateMessage to the leader
        if let Err(err) = self.transport.send(&leader_address, &mut serialized_rsm.as_bytes()) {
            eprintln!("Failed to send RequestStateMessage to leader: {:?}", err);
        }
    }

    fn handle_config_message(&mut self, src: SocketAddr, message: ConfigMessage) {
        println!("{}: received a ConfigMessage {:?} from new leader replica {} with ballot {:?}", self.id, message, message.leader_id, message.config.ballot);

        //Stop leader initialization timeouts
        self.leader_init_timeout.stop();
        self.leader_vote_timeout.stop();
        self.leader_profile_vote_timeout.stop();
        self.leadership_vote_timeout.stop();

        if self.id == message.leader_id && self.role == Role::Leader {
            println!("{}: received a ConfigMessage from itself", self.id);
            return;
        }

        if self.ballot.0 >= message.config.ballot.0 && self.ballot.1 >= message.config.ballot.1 && self.role == Role::Leader {
        // if !self.fresh_ballot(message.config.ballot) && self.role == Role::Leader {
            println!("{}: received a ConfigMessage with a stale op_number and I am leader!", self.id);
            //Affirm leadership
            self.affirm_leadership(self.ballot);
            return;
        }


        if self.ballot.1 > message.config.ballot.1 {
            println!("{}: received a ConfigMessage with a stale op_number ", self.id);
            return; //ignore
        }

        //Update the replica config - later?
        self.config = message.config.clone();

        //let configx = message.config.clone();

        //Use the replica config to update some state
        println!("{}: updating ballot term to {:?}", self.id, self.config.ballot.0);

        self.ballot.0 = self.config.ballot.0; //Only update the term

        //Check the ops... check the op_number and if less than the ballot.1, then request for state

        self.leader = Some((message.leader_id, self.config.ballot));

        //Update the role - but need to check the current role on the replica
        let replicas = self.config.replicas.clone();

        for replica in replicas.iter() {
            if replica.id == self.id {
                match self.role {
                    Role::Witness => {
                        if replica.role == Role::Candidate { // A witness being promoted to candidate
                            //Change the role to candidate
                            println!("{}: changing the role to Candidate - execute & request state", self.id);
                            self.role = replica.role;

                            //Check if the replica needs to execute some entries
                            if self.execution_index != self.commit_index {
                                //Get the log entries
                                let entries = self.log.get_log_entries(self.execution_index, self.commit_index);

                                for entry in entries { 
                                    println!("{}: Log entry - {:?}", self.id, entry);

                                    //Determine the type of operation
                                    let operation = entry.request.operation.clone();
                                    let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&operation);
                                    
                                    match op_result {
                                        Ok(msg) => {
                                            match msg {
                                                Operation::SET(key, value) => {
                                                    // Execute the operation on the kvstore
                                                    let result = self.data.set(key, value);
                                                    
                                                    // Update the execution index
                                                    self.execution_index += 1;

                                                    //Set the log with result of the op
                                                    self.log.set_response_message(entry.ballot.1, result.clone());

                                                    //Set the log with the state executed
                                                    self.log.set_status(entry.ballot.1, LogEntryState::Executed);

                                                    let requests = &mut self.requests;

                                                    if let Some(client_requests) = requests.get_mut(&entry.request.client_id) {
                                                        if let Some(request) = client_requests.get_mut(&entry.request.request_id) {
                                                            request.1 = true;
                                                        }
                                                    }

                                                }
                                                Operation::GET(_key) => {
                                                    //ToDo
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            println!("{}: received err invalid operation request", self.id);
                                        }
                                    }

                                }
                            }

                            // Check if the replica should go to recovery and request for new state
                            if self.ballot.1 < self.config.ballot.1 {
                                // Change status to recovery
                                self.state = RECOVERY;

                                // Send the RequestStateMessage to the leader only if the condition is met
                                self.send_state_request(src);
                            }

                            else {
                                self.state = NORMAL;

                                //Start the poll leader and heartbeat timeouts
                                let _ = self.poll_leader_timeout.reset();
                                let _ = self.heartbeat_timeout.reset();
                                let _ = self.poll_leader_timer.reset();

                            }

                            
                        }

                        else {
                            //Change the state to normal
                            self.state = NORMAL;
                
                            //Start the heartbeat timeout
                            let _ = self.heartbeat_timeout.reset();

                            return;
                        }
                    }

                    Role::Candidate => {
                        if replica.role == Role::Witness {
                            //Change the role to witness
                            println!("{}: changing the role to Witness - request state", self.id);
                            self.role = replica.role;

                            // Check if the replica should go to recovery and request for new state
                            if self.ballot.1 < self.config.ballot.1 {
                                // Change status to recovery
                                self.state = RECOVERY;

                                // Send the RequestStateMessage to the leader only if the condition is met
                                self.send_state_request(src);
                            }

                            else {
                                //Change status to normal
                                self.state = NORMAL;

                                //Start the heartbeat timeout
                                let _ = self.heartbeat_timeout.reset();
                            }
                        }

                        else {
                            //Still a candidate
                            self.role = replica.role;
                            println!("{}: still a candidate [{:?}] - reset the timers", self.id, self.role);

                            //Change the status to normal
                            self.state = NORMAL;

                            //Start the poll leader and heartbeat timeouts
                            let _ = self.poll_leader_timeout.reset();
                            let _ = self.heartbeat_timeout.reset();
                            let _ = self.poll_leader_timer.reset();
                        }
                    }

                    Role::Leader => {
                        //self.ballot = self.config.ballot;
                        //Stop the leader-related timers
                        self.leader_lease_timeout.stop();
                

                        if replica.role == Role::Witness {
                            //Change the role to witness
                            println!("{}: changing the role to Witness - request state?", self.id);
                            self.role = replica.role;

                            //Change status to normal
                            self.state = NORMAL;

                            //Start the heartbeat timeout
                            let _ = self.heartbeat_timeout.reset();
                        }

                        else if replica.role == Role::Candidate {
                            //Change the role to candidate
                            println!("{}: changing role to candidate - reset the timers", self.id);
                            self.role = replica.role;

                            //Change the status to normal
                            self.state = NORMAL;

                            //Start the poll leader and heartbeat timeouts
                            let _ = self.poll_leader_timeout.reset();
                            let _ = self.heartbeat_timeout.reset();
                            let _ = self.poll_leader_timer.reset();
                        }
                    }

                    _ => {
                        //Update the role to the replica role - we need to request state | member
                        println!("{}: changing the role to {:?}", self.id, replica.role);
                        self.role = replica.role;

                        //match on the role
                        match self.role {
                            Role::Witness => {
                                //Change the state to normal
                                self.state = NORMAL;
                    
                                //Start the heartbeat timeout
                                let _ = self.heartbeat_timeout.reset();

                                // //request for the state
                                // let request_state_message = RequestStateMessage {
                                //     ballot: self.ballot,
                                //     commit_index: self.commit_index,
                                // };

                                // //Serialize the RequestStateMessage with meta type set
                                // let serialized_rsm = wrap_and_serialize(
                                //     "RequestStateMessage", 
                                //     serde_json::to_string(&request_state_message).unwrap()
                                // );

                                // //Send the RequestStateMessage to the leader
                                // println!("{}: sending a RequestStateMessage to Replica {} with ballot {:?}", self.id, message.leader_id, self.ballot);

                                // let _ = self.transport.send(
                                //     &src,
                                //     &mut serialized_rsm.as_bytes(),
                                // );
                            
                            }

                            Role::Candidate => {
                                //Change the state to normal
                                self.state = NORMAL;


                                //Start the poll leader and heartbeat timeouts
                                let _ = self.poll_leader_timeout.reset();
                                let _ = self.heartbeat_timeout.reset();
                                let _ = self.poll_leader_timer.reset();


                                // //request for the state
                                // let request_state_message = RequestStateMessage {
                                //     ballot: self.ballot,
                                //     commit_index: self.commit_index,
                                // };

                                // //Serialize the RequestStateMessage with meta type set
                                // let serialized_rsm = wrap_and_serialize(
                                //     "RequestStateMessage", 
                                //     serde_json::to_string(&request_state_message).unwrap()
                                // );

                                // //Send the RequestStateMessage to the leader
                                // println!("{}: sending a RequestStateMessage to Replica {} with ballot {:?}", self.id, message.leader_id, self.ballot);

                                // let _ = self.transport.send(
                                //     &src,
                                //     &mut serialized_rsm.as_bytes(),
                                // );
                            }
                            _ => {
                                panic!("{}: only witness and candidate can be updated", self.id);
                            } //Unreachable
                        }
                    }
                }
            }
        }
    }

    //The leader re(asserts/afirms leadership if with a better ballot than source or if detected offline)
    fn affirm_leadership (&mut self, ballot: (u32, u64)) {
        //Use the ballot to update

        // if self.fresh_ballot(ballot) && self.next_ballot(ballot) {
            println!("{}: Now continuing to assert|affirm leadership", self.id);

            //Increment the ballot
            self.ballot = (ballot.0 + 1, ballot.1);

            //Set the config
            self.config = self.update_config();

            //Create the ConfigMessage
            let config_message = ConfigMessage {
                leader_id: self.id,
                config: self.config.clone(),
            };

            //Serialize the ConfigMessage with meta type set
            let serialized_cm = wrap_and_serialize(
                "ConfigMessage", 
                serde_json::to_string(&config_message).unwrap()
            );

            //Send the ConfigMessage to all replicas
            println!("{}: broadcasting the ConfigMessage to all replicas", self.id);
                let _ = self.transport.broadcast(
                    &self.config,
                    &mut serialized_cm.as_bytes(),
            );
        // }

        // else {
        //     log::info!("{}: stale ballot - ignoring affirm leadership request", self.id);
        // }
        
    }

    fn handle_heartbeat_message(&mut self, src: SocketAddr, message: HeartBeatMessage){

        //assert!(self.ballot == message.ballot, "Only a replica with the same ballot can handle a HeartBeatMessage");
        //assert!(self.role != Role::Leader, "Only a non-leader can handle a HeartBeatMessage"); //ToDo
        //assert!(self.leader.unwrap().0 == message.leader_id, "Only accepting replica with my leader info");

        //Ignore if the heartbeat is from itself
        if self.id == message.leader_id {
            println!("{}: received a HeartBeatMessage from itself", self.id);
            return;
        }


        if self.ballot == message.ballot {
            if self.role == Role::Candidate {

                println!("{}: resetting the HB C timer", self.id);
                //Reset the pollleader timer
                let _ = self.poll_leader_timer.reset();
                //Reset the heartbeat timeout
                let _ = self.heartbeat_timeout.reset();
                //Reset the poll leader timeout
                let _ = self.poll_leader_timeout.reset();

                return;
            }

            else if self.role ==Role::Witness {
                println!("{}: resetting the HB W timer", self.id);
                //Reset the heartbeat timer
                let _ = self.heartbeat_timeout.reset();

                return;
            }

            else {
                //Request for the leader configuration - as the role is a member / Unreachable

                let request_config_message = RequestConfigMessage {
                    replica_id: self.id,
                    replica_address: self.replica_address,
                    ballot: self.ballot,
                };

                //Serialize the RequestConfigMessage with meta type set
                let serialized_rcm = wrap_and_serialize(
                    "RequestConfigMessage", 
                    serde_json::to_string(&request_config_message).unwrap()
                );

                //Send the RequestConfigMessage to the leader
                println!("{}: sending a RequestConfigMessage to Replica {} with my ballot {:?}", self.id, message.leader_id, self.ballot);

                let _ = self.transport.send(
                    &message.leader_address,
                    &mut serialized_rcm.as_bytes(),
                );

                return;

            }
        }

        else if self.ballot < message.ballot {
            // //Request for the leader configuration - as the role is a member / Unreachable

            // let request_config_message = RequestConfigMessage {
            //     replica_id: self.id,
            //     replica_address: self.replica_address,
            //     ballot: self.ballot,
            // };

            // //Serialize the RequestConfigMessage with meta type set
            // let serialized_rcm = wrap_and_serialize(
            //     "RequestConfigMessage", 
            //     serde_json::to_string(&request_config_message).unwrap()
            // );

            // //Send the RequestConfigMessage to the leader
            // println!("{}: sending a RequestConfigMessage to Replica {} with ballot {:?}", self.id, message.leader_id, self.ballot);

            // let _ = self.transport.send(
            //     &message.leader_address,
            //     &mut serialized_rcm.as_bytes(),
            // );

            println!("{}: request for new state in HB", self.id);

            self.send_state_request(src);

        }

        else {
            //Ignore the message
            println!("{}: received a HeartBeatMessage with a stale ballot", self.id);
            return;
        }
    }

    fn handle_poll_leader_message(&mut self, message:PollLeaderMessage) {

        //println!("{}: received a PollLeaderMessage from {}", self.id, message.candidate_id);

        //Ignore if the poll leader is from itself
        if self.id == message.candidate_id {
            println!("{}: ignore received a PollLeaderMessage from itself", self.id);
            return;
        }

        //Ignore if the role is not leader
        if self.role != Role::Leader {
            println!("{}: ignore received a PollLeaderMessage as am a non-leader", self.id);
            return;
        }

        else {

            if self.ballot != message.ballot {
                println!("{}: ignore received a PollLeaderMessage with a different term", self.id);
                return;
            }

            else {
                //Update the liveness map
                self.liveness.insert(message.candidate_id, Instant::now());

                let poll_leader_ok_message = PollLeaderOkMessage {
                    leader_id: self.id,
                    ballot: self.ballot,
                    replica_profile: {
                        if let Some(profile) = self.monitor.get((message.candidate_id - 1) as usize) { profile.get_x() }
                        else { 0 }
                    },
                };

                //Serialize the PollLeaderOkMessage with meta type set
                let serialized_plok = wrap_and_serialize(
                    "PollLeaderOkMessage", 
                    serde_json::to_string(&poll_leader_ok_message).unwrap()
                );

                //Send the PollLeaderOkMessage to the candidate
                println!("{}: sending a PollLeaderOkMessage to Replica {} with ballot {:?}", self.id, message.candidate_id, self.ballot);

                let _ = self.transport.send(
                    &message.candidate_address,
                    &mut serialized_plok.as_bytes(),
                );

                return;
            }
        }
    }

    fn handle_poll_leader_ok_message(&mut self, message: PollLeaderOkMessage){

        //println!("{}: received a PollLeaderOkMessage from leader {}", self.id, message.leader_id);

        //Ignore if the pollleaderokmessage is from itself
        if self.id == message.leader_id {
            println!("{}: ignore as the PollLeaderOkMessage is from itself", self.id);
            return;
        }

        //Process if the role is candidate
        if self.role == Role::Candidate {

            if self.ballot == message.ballot {

                let p1  = message.replica_profile;
                let p2 = {
                    if let Some(profile) = self.monitor.get((message.leader_id - 1) as usize) { profile.get_x() }
                    else { 0 }
                    
                };

                //println!("{}: p2 = {}, p1 = {}", self.id, p1, p2);

                if p2 > p1 {
                    //Check the variation in the profiles

                    if (p2 - p1) >= 6 {
                        //Change state to election
                        self.state = ELECTION;

                        //Start the election cycle with type profile
                        self.start_election_cycle(ElectionType::Profile);
                    }

                    else {
                        //Reset the timers
                        println!("{}: resetting the PL timers", self.id);

                        //Reset the pollleader timer
                        let _ = self.poll_leader_timeout.reset();

                        //Reset the heartbeat timeout
                        let _ = self.heartbeat_timeout.reset();

                        //Reset the poll leader timeout
                        let _ = self.poll_leader_timeout.reset();

                        return;
                    }
                }

                else {
                    //Reset the timers
                    println!("{}: resetting the PL timers", self.id);

                    //Reset the pollleader timer
                    let _ = self.poll_leader_timeout.reset();

                    //Reset the heartbeat timeout
                    let _ = self.heartbeat_timeout.reset();

                    //Reset the poll leader timeout
                    let _ = self.poll_leader_timeout.reset();

                    return;
                }
            }

            else {
                //Ignore for other terms
                println!("{}: ignore received PollLeaderOkMessage with a different term", self.id);
                return;
            }
        }

        else {
            println!("{}: ignore received a PollLeaderOkMessage but am a non-candidate", self.id);
            return;
        }
    }

    fn handle_request_config(&mut self, message: RequestConfigMessage){
        if self.role != Role::Leader {
            //Ignore if the replica roler here is the leader
            println!("{}: received a RequestConfigMessage but am a non-leader", self.id);
            return;
        }

        else {
            //Check if the replica is part of the current configuration

            for replica in self.config.replicas.iter(){
                if replica.id == message.replica_id { 
                    //Create the ConfigMessage
                    let config_message = ConfigMessage {
                        leader_id: self.id,
                        config: self.config.clone(),
                    };

                    //Serialize the ConfigMessage with meta type set
                    let serialized_cm = wrap_and_serialize(
                        "ConfigMessage", 
                        serde_json::to_string(&config_message).unwrap()
                    );

                    //Send the ConfigMessage to the replica
                    println!("{}: sending a ConfigMessage to Replica {} with ballot {:?}", self.id, message.replica_id, self.ballot);

                    let _ = self.transport.send(
                        &message.replica_address,
                        &mut serialized_cm.as_bytes(),
                    );

                    return;
                }

                else {
                    continue;
                }

            }
        }

        //Implies we have a new replica and the configuration needs to be updated
        let mut config = self.config.clone();

        let n = config.n + 1;
        let f = config.f + 1;

        config.n = n;
        config.f = f;


        let r = Replica {
            id: message.replica_id,
            replica_address: message.replica_address,
            profile: {
                if let Some(profile) = self.monitor.get((message.replica_id - 1) as usize) { profile.get_x() }
                else { 0 }
            },
            role: Role::Member,
            status: INITIALIZATION,

        };

        let mut replicas = config.replicas.clone();
        replicas.push(r);

        //Broadcast the new configuration to all replicas
        let new_config_message = ConfigMessage {
            leader_id: self.id,
            config: config.clone(),
        };

        //Serialize the ConfigMessage with meta type set
        let serialized_ncm = wrap_and_serialize(
            "ConfigMessage", 
            serde_json::to_string(&new_config_message).unwrap()
        );

        //Send the ConfigMessage to all replicas
        println!("{}: broadcasting the ConfigMessage to all replicas", self.id);
        let _ = self.transport.broadcast(
            &config,
            &mut serialized_ncm.as_bytes(),
        );

        return;

    }
    
    fn handle_leader_init_timeout (&mut self) {
        println!("{}: received LeaderInitTimeout - start normal election", self.id);
        //Start the election cycle with type normal
        self.state = ELECTION;
        self.start_election_cycle(ElectionType::Normal);
    }

    fn handle_leader_vote_timeout (&mut self) {
        println!("{}: received LeaderVoteTimeout", self.id);
        //Start the election cycle type start_election_cycle(type: timeout)
        self.state = ELECTION;
        self.start_election_cycle(ElectionType::Timeout);
    }

    fn handle_leader_profile_vote_timeout (&mut self) {
        println!("{}: received LeaderProfileVoteTimeout", self.id);
        //Assert leadership position
        self.state = NORMAL;
        self.affirm_leadership(self.ballot);
    }

    fn handle_leadership_vote_timeout (&mut self) {
        println!("{}: received LeadershipVoteTimeout", self.id);
        self.state = ELECTION;
        //Start the election cycle type start_election_cycle(id, election_type: degraded)
        self.start_election_cycle(ElectionType::Degraded);
    }

    fn handle_leader_lease_timeout (&mut self) {
        println!("{}: received leader lease timeout", self.id);

        if self.role == Role::Leader {

            //Send heartbeat to each replica in the system

            for replica in self.config.replicas.iter(){
                if replica.id != self.id {
                    //Create the HeartBeatMessage
                    let heartbeat_message = HeartBeatMessage {
                        leader_id: self.id,
                        leader_address: self.replica_address,
                        ballot: self.ballot,
                        replica_profile: {
                            if let Some(profile) = self.monitor.get((replica.id - 1) as usize) { profile.get_x() }
                            else { 0 }
                        },
                    };

                    //Serialize the HeartBeatMessage with meta type set
                    let serialized_hbm = wrap_and_serialize(
                        "HeartBeatMessage", 
                        serde_json::to_string(&heartbeat_message).unwrap()
                    );

                    //Send the HeartBeatMessage to a replica
                    //println!("{}: sending a HeartBeatMessage to Replica {} with ballot {:?}", self.id, replica.id, self.ballot);
                    let _ = self.transport.send(
                        &replica.replica_address,
                        &mut serialized_hbm.as_bytes(),
                    );
                }
            }
        }

        else {
            println!("{}: ignore as this message is only for the leader", self.id);
        }

        //Reset the leadership lease timeout
        if self.leader_lease_timeout.active() {
            let _ = self.leader_lease_timeout.reset();
        }
        else {
            let _ = self.leader_lease_timeout.start();
        }

    }

    fn handle_poll_leader_timeout(&mut self){
        //Stop the poll_leader_timeout(r)
        //let _ = self.poll_leader_timeout.stop();
        //let _ = self.poll_leader_timer.stop();

        println!("{}: received PollLeaderTimeout at {:?}", self.id, Instant::now());
        //Start the election cycle type Timeout
        self.state = ELECTION;
        self.start_election_cycle(ElectionType::Offline);

    }

    fn handle_heartbeat_timeout(&mut self){
        println!("{}: received HeartBeatTimeout", self.id);
        //Start the election cycle type Timeout
        self.state = ELECTION;
        self.start_election_cycle(ElectionType::Offline);
    }

    fn handle_poll_leader_timer(&mut self){
        //Stop the poll leader timer
        let _ = self.poll_leader_timer.stop();
        
        println!("{}: received PollLeaderTimer[Out] - check on the leader at {:?}", self.id, Instant::now());
        //Poll the leader for liveness

        if self.role != Role::Candidate {
            //Ignore as this is done only the candidate
            println!("{}: ignore as this is done only at the candidate", self.id);
            return;
        }

        else {
            //Candidate polls the leader
            let poll_leader_message = PollLeaderMessage {
                candidate_id: self.id,
                candidate_address: self.replica_address,
                ballot: self.ballot,
            };

            //Serialize the PollLeaderMessage with meta type set
            let serialized_plm = wrap_and_serialize(
                "PollLeaderMessage", 
                serde_json::to_string(&poll_leader_message).unwrap()
            );

            //Send the PollLeaderMessage to the leader
            //println!("{}: sending a PollLeaderMessage to Replica {} with term {} at {:?}", self.id, self.leader.unwrap().0, self.ballot.1, Instant::now());

            //Get the leader address
            let leader_address = {
                if let Some(leader) = self.config.replicas.iter().find(|r| r.id == self.leader.unwrap().0) { leader.replica_address.clone() }
                else { self.replica_address.clone() }
            };
            
            let _ = self.transport.send(
                &leader_address,
                &mut serialized_plm.as_bytes(),
            );

            return;
        }
    }

    fn _perform_liveness_check(&mut self) {
        let mut to_remove = Vec::new();

        for (replica_id, last_seen) in &self.liveness {
            //Get the role of the replica in the config
            let role = {
                if let Some(replica) = self.config.replicas.iter().find(|r| r.id == *replica_id) { replica.role }
                else { Role::Member }
            };

            if last_seen.elapsed().as_secs() >= 5 && role == Role::Candidate { //Check the role of the replica - should be candidate
                println!("{}: Replica {} is offline", self.id, replica_id);
               to_remove.push(replica_id.clone());
            }
        }

        for replica_id in to_remove {
            self.liveness.remove(&replica_id);
            self.config.remove_replica(replica_id)
        }
    
        // Update the configuration after all modifications
        self.config = self.update_config();
    
        // Create the ConfigMessage
        let config_message = ConfigMessage {
            leader_id: self.id,
            config: self.config.clone(),
        };
    
        // Serialize the ConfigMessage with meta type set
        let serialized_cm = wrap_and_serialize(
            "ConfigMessage", 
            serde_json::to_string(&config_message).unwrap()
        );
    
        // Send the ConfigMessage to all replicas
        println!("{}: broadcasting the ConfigMessage to all replicas", self.id);
        let _ = self.transport.broadcast(
            &self.config,
            &mut serialized_cm.as_bytes(),
        );
    }
    
    //Request Processing Handlers
    fn handle_request_message(&mut self, message: RequestMessage) {
        //Need to get the message type and act accordingly
        println!("{}: received a request message from client {} with request id {}", self.id, message.client_id, message.request_id);

        if self.role == Role::Leader && self.state == NORMAL {
            let c_id = message.client_id;
            let c_req_id = message.request_id;


            //Get all the requests at the replica from this client c_id
            let requests = self.requests.get(&c_id);


            //Check if there are requests for this client
            if requests.is_some() {

                //Check if the request_id has been logged before
                let request = requests.unwrap().get(&c_req_id);

                if request.is_some() {
                    //Get the operation number and status of the request
                    let op_num = request.unwrap().0;
                    let status = request.unwrap().1;

                    if status { //Has been executed before
                        //Get the entry from the log by operation number
                        let entry = self.log.find(op_num).unwrap();

                        //Get the reply from the log
                        let reply = entry.response.clone().unwrap();

                        //Send the ResponseMessage to the client
                        let response_message = ResponseMessage {
                            request_id: c_req_id,
                            reply: reply.to_bytes().unwrap(),
                        };

                        //Serialize the ResponseMessage with meta type set
                        let serialized_rm = wrap_and_serialize(
                            "ResponseMessage", 
                            serde_json::to_string(&response_message).unwrap()
                        );

                        //Send the ResponseMessage to the client
                        println!("{}: sending a ResponseMessage to Client {} with request id {} at {}", self.id, c_id, c_req_id, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
                        let _ = self.transport.send(
                            &c_id,
                            &mut serialized_rm.as_bytes(),
                        );

                        return;
                    }

                    else { 
                        // Request still being processed
                        println!("{}: request still being processed", self.id); // Resend the Proposal request to the replicas

                        //Send the ProposeMessage to the peers
                        let propose_message = ProposeMessage {
                            ballot: (self.ballot.0, op_num),
                            request: message,
                        };

                        //Serialize the ProposeMessage with meta type set
                        let serialized_pm = wrap_and_serialize(
                            "ProposeMessage", 
                            serde_json::to_string(&propose_message).unwrap()
                        );

                        //Send the ProposeMessage to all replicas
                        println!("{}: broadcasting the ProposeMessage to all replicas", self.id);
                        let _ = self.transport.broadcast(
                            &self.config,
                            &mut serialized_pm.as_bytes(),
                        );

                        return;
                    }
                }

                else { //No request for the client with the request id
                    log::info!("{}: no request for the client with the request id {}", self.id, c_req_id);

                    //Increment the last_op
                    self.last_op += 1;

                    //Get the op_result
                    let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&message.operation);

                    match op_result {
                        Ok(msg) => {
                            match msg {
                                Operation::SET(_key, _value) => {

                                    //Append the request_map to the requests
                                    self.requests
                                        .entry(c_id)
                                        .or_insert_with(HashMap::new)
                                        .insert(c_req_id, (self.last_op, false));
                                    
                                    //self.requests.insert(c_id, request_map);
    
                                    //Start the Paxos-related
                                    let ballot = (self.ballot.0, self.last_op);
    
                                    //Append the entry to the log
                                    self.log.append(ballot, message.clone(), None, LogEntryState::Propose);
    
                                    //Send the ProposeMessage to the peers
                                    let propose_message = ProposeMessage {
                                        ballot: ballot,
                                        request: message,
                                    };
    
                                    //Serialize the ProposeMessage with meta type set
                                    let serialized_pm = wrap_and_serialize(
                                        "ProposeMessage", 
                                        serde_json::to_string(&propose_message).unwrap()
                                    );
    
                                    //Send the ProposeMessage to all replicas
                                    println!("{}: broadcasting the ProposeMessage to all replicas", self.id);
                                    let _ = self.transport.broadcast(
                                        &self.config,
                                        &mut serialized_pm.as_bytes(),
                                    );
                                }
    
                                Operation::GET(_key) => {
    
                                }
                            }
                        }
    
                        Err(_e) => {
                            println!("{}: error deserializing the operation", self.id);
                        }
                    }
    
                }

            }

            else {

                //Increment the last_op
                self.last_op += 1;

                //Get the op_result
                let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&message.operation);

                match op_result {
                    Ok(msg) => {
                        match msg {
                            Operation::SET(_key, _value) => {

                                //Update the request map for this client
                                self.requests
                                        .entry(c_id)
                                        .or_insert_with(HashMap::new)
                                        .insert(c_req_id, (self.last_op, false));

                                //Start the Paxos-related
                                let ballot = (self.ballot.0, self.last_op);

                                //Append the entry to the log
                                self.log.append(ballot, message.clone(), None, LogEntryState::Propose);

                                //Send the ProposeMessage to the peers
                                let propose_message = ProposeMessage {
                                    ballot: ballot,
                                    request: message,
                                };

                                //Serialize the ProposeMessage with meta type set
                                let serialized_pm = wrap_and_serialize(
                                    "ProposeMessage", 
                                    serde_json::to_string(&propose_message).unwrap()
                                );

                                //Send the ProposeMessage to all replicas
                                println!("{}: broadcasting the ProposeMessage to all replicas", self.id);
                                let _ = self.transport.broadcast(
                                    &self.config,
                                    &mut serialized_pm.as_bytes(),
                                );
                            }

                            Operation::GET(_key) => {

                            }
                        }
                    }

                    Err(_e) => {
                        println!("{}: error deserializing the operation", self.id);
                    }
                }

            }
        }

        else {
            //Forward Request to the leader
            if self.leader.is_some() && self.leader.unwrap().0 != self.id { //Only forward requests from a non-leader
                let leader_id = self.leader.unwrap().0;
                let leader_address = {
                    if let Some(leader) = self.config.replicas.iter().find(|r| r.id == leader_id) { leader.replica_address.clone() }
                    else { self.replica_address.clone() }
                };

                //Serialize the RequestMessage with meta type set
                let serialized_rm = wrap_and_serialize(
                    "RequestMessage", 
                    serde_json::to_string(&message).unwrap()
                );

                //Send the RequestMessage to the leader
                println!("{}: forwarding the RequestMessage to leader replica {}", self.id, leader_id);

                let _ = self.transport.send(
                    &leader_address,
                    &mut serialized_rm.as_bytes(),
                );
            }

            else {
                println!("{}: ignore as there is no leader", self.id);
                return;
            }
        }

    }

    fn handle_propose_message(&mut self, src: SocketAddr, message: ProposeMessage) {

        if self.role == Role::Leader {
            println!("{}: ignore as this is done by non-leader nodes and my role is {:?}", self.id, self.role);
            return;
        }

        if self.state != NORMAL {
            println!("{}: ignore as this propose is done only in the normal state", self.id);
            return;
        }

        //Stale Propose Message
        if message.ballot < self.ballot {
            println!("{}: ignore as this propose message is stale < ", self.id);
            return;
        }

        //Future Propose Message
        else if message.ballot > self.ballot {
            //Determine if next proposal
            if message.ballot.0 == self.ballot.0 && message.ballot.1 == (self.ballot.1 + 1) {
                //Vote procedures: //Check if the request is in the log
                let request_entry = self.log.find(message.ballot.1);

                if request_entry.is_some() {
                    println!("{}: request already in the log", self.id);

                    let entry_state = &request_entry.unwrap().state;

                    match entry_state {
                        LogEntryState::Proposed => {
                            //Resend the ProposeOKMessage to the leader

                            let propose_ok_message = ProposeOkMessage {
                                ballot: message.ballot,
                                commit_index: self.commit_index,
                            };

                            //Serialize the ProposeOkMessage with meta type set
                            let serialized_pom = wrap_and_serialize(
                                "ProposeOkMessage", 
                                serde_json::to_string(&propose_ok_message).unwrap()
                            );

                            //Send the ProposeOkMessage to the leader
                            println!("{}: sending a ProposeOkMessage to leader {} with ballot {:?}", self.id, src, message.ballot);

                            let _ = self.transport.send(
                                &src,
                                &mut serialized_pom.as_bytes(),
                            );
                        }
                        _ => {}
                    }
                }

                else {

                    //Update the last_op
                    //self.last_op += 1;

                    //Add the request to the log
                    self.log.append(message.ballot, message.request.clone(), None, LogEntryState::Proposed);

                    println!("{}: added request to the log - sending ProposeOk", self.id);

                    //Send the ProposeOkMessage to the leader
                    let propose_ok_message = ProposeOkMessage {
                        ballot: message.ballot,
                        commit_index: self.commit_index,
                    };

                    //Serialize the ProposeOkMessage with meta type set
                    let serialized_pom = wrap_and_serialize(
                        "ProposeOkMessage", 
                        serde_json::to_string(&propose_ok_message).unwrap()
                    );

                    //Send the ProposeOkMessage to the leader
                    println!("{}: sending a ProposeOkMessage to Replica {} with ballot {:?}", self.id, src, message.ballot);
                    let _ = self.transport.send(
                        &src,
                        &mut serialized_pom.as_bytes(),
                    );
                }
            }

            else {
                //Request for new state
                println!("{}: request for new state in propose: my ballot {:?} v leader ballot {:?}", self.id, self.ballot, message.ballot);
                // Change status to recovery
                self.state = RECOVERY;

                // Send the RequestStateMessage to the leader only if the condition is met
                self.send_state_request(src);
            }
        }

        // The messages are similar - resend the response
        else {
            println!("{}: ignore as this propose message is stale == ", self.id);
            return;
        }
    }

    fn handle_propose_ok(&mut self, src: SocketAddr, message: ProposeOkMessage) {

        println!("{}: received a ProposeOkMessage from Replica {} with ballot {:?}", self.id, src, message.ballot);

        if self.role == Role::Leader && self.state == NORMAL {
            
            //if message.ballot == self.ballot {
                //println!("{}: received a ProposeOkMessage from Replica {} with ballot {:?}", self.id, src, message.ballot);
                
                //Update the liveness instant for each replica
                //Use the src to get the id of the replica
                let replica_id = {
                    if let Some(replica) = self.config.replicas.iter().find(|r| r.replica_address == src) { replica.id }
                    else { self.id }
                };

                //Update the liveness instant for the replica
                self.liveness.insert(replica_id, Instant::now()); //Done at the candidate
                
    
                // Find the request in the log
                let ballot = message.ballot.clone();
    
                if let Some(entry) =  self.log.find(ballot.1) { //last_op!!
                    println!("{}: Log entry with last op {} found", self.id, ballot.1);
                    if entry.state == LogEntryState::Propose {
                        // Add the ProposeOkMessage to the quorum
                        if let Some(_msgs) = self.propose_quorum.add_and_check_for_quorum(ballot.clone(), src, message.clone()) {
                            println!("{}: received a quorum of ProposeOkMessages", self.id);
    
                            // Clone entry before mutating self.log
                            let entry_clone = entry.clone();
    
                            // Update the log with committed
                            self.log.set_status(ballot.1, LogEntryState::Committed);
    
                            // Update the current ballot & commit index
                            // self.ballot.1 += 1;
                            self.ballot.1 = ballot.1; //op_number
                            self.commit_index = self.ballot.1;
    
                            // Determine the type of operation
                            let operation = entry_clone.request.operation.clone();
                            let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&operation);
                            
                            match op_result {
                                Ok(msg) => {
                                    match msg {
                                        Operation::SET(key, value) => {
                                            
                                            // Execute the operation
                                            let result = self.data.set(key, value);

                                            //print the result
                                            println!("{}: result of the operation is {:?}", self.id, result);

                                            //Set the log with result of the op
                                            self.log.set_response_message(ballot.1, result.clone());

                                            //Set the log with the state executed
                                            self.log.set_status(ballot.1, LogEntryState::Executed);

                                            //Update the execution index
                                            self.execution_index = self.commit_index;

                                            // //Send the reply to the client
                                            // println!("{}: sending a ResponseMessage to Client {} with request id {}", self.id, entry_clone.request.client_id, entry_clone.request.request_id);

                                            // let _ = self.transport.send(
                                            //     &entry_clone.request.client_id,
                                            //     &result.to_bytes().unwrap(),
                                            // );

                                            //Send the ResponseMessage to the client
                                            let response_message = ResponseMessage {
                                                request_id: entry_clone.request.request_id,
                                                reply: result.to_bytes().unwrap(),
                                            };

                                            //Serialize the ResponseMessage with meta type set
                                            let serialized_rm = wrap_and_serialize(
                                                "ResponseMessage", 
                                                serde_json::to_string(&response_message).unwrap()
                                            );

                                            //Send the ResponseMessage to the client
                                            println!("{}: sending a ResponseMessage to Client {} with request id {} at {}", self.id, entry_clone.request.client_id, entry_clone.request.request_id, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
                                            let _ = self.transport.send(
                                                &entry_clone.request.client_id,
                                                &mut serialized_rm.as_bytes(),
                                            );

                                            let requests = &mut self.requests;

                                            if let Some(client_requests) = requests.get_mut(&entry_clone.request.client_id) {
                                                if let Some(request) = client_requests.get_mut(&entry_clone.request.request_id) {
                                                    request.1 = true;
                                                }
                                            }

                                            //Send CommitMessage to all replicas
                                            let commit_message = CommitMessage {
                                                ballot: ballot,
                                            };

                                            //Serialize the CommitMessage with meta type set
                                            let serialized_cm = wrap_and_serialize(
                                                "CommitMessage", 
                                                serde_json::to_string(&commit_message).unwrap()
                                            );

                                            //Send the CommitMessage to all replicas
                                            println!("{}: broadcasting the CommitMessage to all replicas", self.id);
                                            let _ = self.transport.broadcast(
                                                &self.config,
                                                &mut serialized_cm.as_bytes(),
                                            );

                                        }
                                        Operation::GET(_key) => {
                                            //ToDo
                                        }
                                    }
                                }
                                Err(_) => {
                                    println!("{}: received err invalid operation request", self.id);
                                }
                            }
                        }
                            //Perform the liveness check
                            //self.perform_liveness_check();
                    }
                }

                else {
                    log::info!("{}: ignore as the request has not been logged", self.id);
                }
            //}

            //Print the log & data
            //println!("{}: log is {:?}", self.id, self.log);
            //println!("{}: data is {:?}", self.id, self.data);


        } else {
            println!("{}: ignore as this is done ONLY by a leader in NORMAL state", self.id);
        }
    }

    fn handle_commit(&mut self, src: SocketAddr, message: CommitMessage) {
        println!("{}: received a CommitMessage from replica {} with commit ballot {:?}. My ballot: {:?}", self.id, src, message.ballot, self.ballot);

        if self.role == Role::Leader || self.role == Role::Member {
            println!("{}: ignore as this is done by non-leader/member nodes but my role is {:?}", self.id, self.role);
            return;
        }

        if self.state != NORMAL {
            println!("{}: ignore as this commit is done only in the normal state", self.id);
            return;
        }

        //Stale ballot
        if message.ballot < self.ballot {
            println!("{}: ignore as the commit message is stale < ", self.id);
            return;
        }

        else if message.ballot > self.ballot {
            //Determine if next ballot
            if message.ballot.0 == self.ballot.0 && message.ballot.1 == self.ballot.1 + 1 {
                // Find the request in the log
                let ballot = message.ballot.clone();

                if let Some(entry) = self.log.find(ballot.1) {
                    if entry.state == LogEntryState::Proposed {
                        //Create the log clone to work with
                        let entry_clone = entry.clone(); //for immutable operation

                        //Update the log with committed
                        self.log.set_status(ballot.1, LogEntryState::Committed);

                        //Update the current ballot & commit index
                        //self.ballot.1 += 1;
                        self.last_op += 1;
                        self.ballot.0 = ballot.0;
                        self.ballot.1 = ballot.1; //op_number
                        self.commit_index = self.ballot.1;

                        match self.role {
                            Role::Candidate => {
                                // Determine the type of operation
                                let operation = entry_clone.request.operation.clone();
                                let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&operation);

                                match op_result {
                                    Ok(msg) => {
                                        match msg {
                                            Operation::SET(key, value) => {
                                            
                                                // Execute the operation
                                                let result = self.data.set(key, value);

                                                // Update the execution index
                                                self.execution_index = self.commit_index;

                                                //Set the log with result of the op
                                                self.log.set_response_message(ballot.1, result.clone());

                                                //Set the log with the state executed
                                                self.log.set_status(ballot.1, LogEntryState::Executed);
                                            }
                                            Operation::GET(_key) => {
                                                //ToDo
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        println!("{}: received err invalid operation request", self.id);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }

                else {
                    println!("{}: ignore as the request has not been logged", self.id);
                    return;
                }
            }

            else {
                //Request for new state
                println!("{}: request for new state in commit", self.id);
                // Change status to recovery
                self.state = RECOVERY;

                // Send the RequestStateMessage to the leader only if the condition is met
                self.send_state_request(src);
            }
            
        }

        //Stale ballot - seen
        else {
            println!("{}: ignore as the commit message is stale == ", self.id);
            return;
        }

    }
    

    fn handle_request_state (&mut self, src: SocketAddr, message: RequestStateMessage) {
        // if self.role != Role::Leader || self.state != NORMAL { 
        //     println!("{}: ignore as this is done by normal leader node", self.id);
        //     return;
        // }

        //Just ensure all entries are in the range []ToDox

        if self.commit_index < message.commit_index {
            println!("{}: ignore as local commit is behind", self.id);
            return;
        }

        if self.commit_index == message.commit_index {
            println!("{}: ignore as the local and remote commit indices are similar", self.id);
            return;
        }

        if self.fresh_ballot(message.ballot) {
            println!("{}: ignore as the request state message is future: our ballot - {:?} & their ballot - {:?}", self.id, self.ballot, message.ballot); //Stale leader

            //Change state to recovery //ToDo - who is the leader? Wait..
            self.state = CONFIGURATION;

            return;
        }

        else {

            //Retrieve the log from message.commit_index to our current commit_index

            println!("{}: retrieving log entries from {} to {} for {:?}", self.id, message.commit_index, self.commit_index, src);
            let log = self.log.get_entries(message.commit_index, self.commit_index);

            let len = log.len();

            //Serialize the log
            let log = serde_json::to_vec(&log).unwrap();

            //Send the state to the replica
            let state_message = LogStateMessage {
                ballot: self.ballot,
                commit_index: self.commit_index,
                log: log,
                profile: false,
            };

            //Serialize the LogStateMessage with meta type set
            let serialized_sm = wrap_and_serialize(
                "LogStateMessage", 
                serde_json::to_string(&state_message).unwrap()
            );

            //Send the LogStateMessage to the replica
            println!("{}: sending a LogStateMessage to Replica {} with ballot {:?} and length {}", self.id, src, message.ballot, len);
            let _ = self.transport.send(
                &src,
                &mut serialized_sm.as_bytes(),
            );
        }
    }

    fn handle_poll_monitor_timeout(&mut self) {
        //println!("{}: received PollMonitorTimeout at {:?}", self.id, Instant::now());

        //Create the message
        let request_profile_message = RequestProfileMessage {
            id: self.id,
        };

        //Serialize the RequestProfileMessage with meta type set
        let serialized_rpm = wrap_and_serialize(
            "RequestProfileMessage", 
            serde_json::to_string(&request_profile_message).unwrap()
        );

        //Send the RequestProfileMessage to the monitor
        //println!("{}: sending a RequestProfileMessage to the monitor", self.id);
        let _ = self.transport.send(
            &self.monitor_address,
            &mut serialized_rpm.as_bytes(),
        );

        //Reset the poll monitor timeout
        let _ = self.poll_monitor_timeout.reset();

    }

    fn handle_response_profile_message(&mut self, message: ResponseProfileMessage) {
        //println!("{}: received a ResponseProfileMessage - updating the monitor matrix", self.id);

        //print the profiles received at the node
        //println!("{}: profiles received at the node are {:?}", self.id, message.profiles);

        //Update the monitor
        self.monitor = message.profiles;

        //println!("{}: Monitor profiles {:?}", self.id, self.monitor);

        //Restart the monitor timeout
        let _ = self.poll_monitor_timeout.reset();
    }

    fn handle_received_state(&mut self, src:SocketAddr, message:LogStateMessage) {
        println!("{}: handling received state from {:?}", self.id, src);

        if message.ballot < self.ballot {
            println!("{}: ignore as the state message ballot {:?} is stale to our ballot {:?}", self.id, message.ballot, self.ballot);
            return;
        }

        if message.ballot.1 == self.ballot.1 || message.ballot.1 == self.commit_index || message.ballot.1 == self.execution_index || message.commit_index == self.commit_index{
            println!("{}: ignore as the received state and local state are similar", self.id);
            self.state = NORMAL;
            return;
        }

        if self.state == INITIALIZATION || self.state == CONFIGURATION || self.state == NORMAL || self.state == OFFLINE{
            println!("{}: ignore as this is done in recovery and election states but my state is {}", self.id, self.state);
            return;
        }

        let log_entries: Vec<LogEntry> = serde_json::from_slice(&message.log).unwrap();

        println!("{}: number of logstate entries received: {}", self.id, log_entries.len());

        //Get the first entry in the log
        let first = log_entries.first().unwrap();

        //Get the last entry in the log
        let last = log_entries.last().unwrap();

        //Get the last committed entry in the log of this replica
        let last_commit = self.commit_index;

        //First entry must be <= last_commit
        //Last entry must be >= last_commit
        println!("{}: - first entry in the log {:?}, last entry in the log {:?}, local last commit {:?}", self.id, first, last, last_commit);

        if first.ballot.1 > last_commit {
            println!("{}: ignore as the state message is future", self.id);
            return;
        }

        if last.ballot.1 < last_commit {
            println!("{}: ignore as the state message is stale", self.id);
            return;
        }

        for entry in log_entries.iter() {
            //Check if the entry is already in the log - this will skip the first 

            let request_entry = self.log.find(entry.ballot.1);

            if request_entry.is_some() { // Check the state to ensure it has been exec|comm
                println!("{}: this entry is already in the log {:?} with the state {:?}",self.id, entry, request_entry.unwrap().state);

                // Check for the state of the log | Candidate -> execute, Witness -> only commit

                match self.role {
                    Role::Candidate => {
                        match request_entry.unwrap().state {
                            LogEntryState::Executed => {
                                continue;
                            }
                            LogEntryState::Committed => { //Execute the entry & update the execution index
                                let operation = entry.request.operation.clone();
                                let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&operation);
                            
                                match op_result {
                                    Ok(msg) => {
                                        match msg {
                                            Operation::SET(key, value) => {
                                                println!("{}: now executing received log - Entry {:?}", self.id, entry);
                                                // Execute the operation on the kvstore
                                                let result = self.data.set(key, value);
                                            
                                                // Update the execution index
                                                self.execution_index += 1;

                                                //Set the log with result of the op
                                                self.log.set_response_message(entry.ballot.1, result.clone());

                                                //Set the log with the state executed
                                                self.log.set_status(entry.ballot.1, LogEntryState::Executed);

                                                continue;

                                            }
                                            Operation::GET(_key) => {
                                                //ToDo
                                                continue;
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        panic!("{}: received err invalid operation request", self.id);
                                    }
                                }
                            }

                            _ => {
                                //Commit & execute
                                println!("{}: now commiting received log [candidate] - Entry {:?}", self.id, entry);
                                self.log.set_status(entry.ballot.1, LogEntryState::Committed);

                                self.commit_index += 1;
                                self.last_op += 1;
                                self.ballot.1 +=1;

                                //Determine the type of operation
                                let operation = entry.request.operation.clone();
                                let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&operation);
                                    
                                match op_result {
                                    Ok(msg) => {
                                        match msg {
                                            Operation::SET(key, value) => {
                                                println!("{}: now executing received log [candidate] - Entry {:?}", self.id, entry);
                                                // Execute the operation on the kvstore
                                                let result = self.data.set(key, value);
                                                    
                                                // Update the execution index
                                                self.execution_index += 1;

                                                //Set the log with result of the op
                                                self.log.set_response_message(entry.ballot.1, result.clone());

                                                //Set the log with the state executed
                                                self.log.set_status(entry.ballot.1, LogEntryState::Executed);

                                                continue;

                                            }
                                            Operation::GET(_key) => {
                                                //ToDo
                                                continue;
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        panic!("{}: received err invalid operation request", self.id);
                                    }
                                }

                            }

                        }
                    }

                    Role::Witness => {
                        match request_entry.unwrap().state {
                            LogEntryState::Executed => {
                                continue;
                            }
                            LogEntryState::Committed => {
                                continue;
                            }
                            LogEntryState::Request => {
                                panic!("{}: unsupported op with the state 'request' in the log",self.id);
                            }
                            _ => { //Commit the operation
                                //Update the entry to the log with status committed
                                println!("{}: now commiting received log [witness] - Entry {:?}", self.id, entry);
                                self.log.set_status(entry.ballot.1, LogEntryState::Committed);

                                self.commit_index += 1;
                                self.last_op += 1;
                                self.ballot.1 +=1;

                                continue;
                            }

                            // _ => {
                            //     panic!("{}: the witness only supports executed, committed and proposed entries. Received op with status {:?} for the entry {:?}", self.id, request_entry.unwrap().state, entry);
                            // }

                        }
                    }

                    _ => {
                        println!("{}: replica has no role defined - requesting for configuration", self.id);

                        //Request for configuration
                        let request_config_message = RequestConfigMessage {
                            replica_id: self.id,
                            replica_address: self.replica_address,
                            ballot: self.ballot,
                        };
        
                        //Serialize the RequestConfigMessage with meta type set
                        let serialized_rcm = wrap_and_serialize(
                            "RequestConfigMessage", 
                            serde_json::to_string(&request_config_message).unwrap()
                        );
        
                        //Send the RequestConfigMessage to the leader
                        println!("{}: sending a RequestConfigMessage to replica {:?} with my ballot {:?}", self.id, src, self.ballot);
        
                        let _ = self.transport.send(
                            &src,
                            &mut serialized_rcm.as_bytes(),
                        );
        
                        return;
                    }
                }
            }

            else {
                //Update the commit index
                println!("{}: now commiting received log [all - new] - Entry {:?}", self.id, entry);
                self.commit_index += 1;
                self.last_op += 1;
                self.ballot.1 +=1;

                //Add the entry to the log with status committed
                self.log.append(entry.ballot, entry.request.clone(), entry.response.clone(), LogEntryState::Committed);

                //Match the node role
                match self.role {
                    Role::Candidate => {
                        //Determine the type of operation
                        let operation = entry.request.operation.clone();
                        let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&operation);
                            
                        match op_result {
                            Ok(msg) => {
                                match msg {
                                    Operation::SET(key, value) => {
                                        println!("{}: now executing received log [candidate - new] - Entry {:?}", self.id, entry);
                                        // Execute the operation on the kvstore
                                        let result = self.data.set(key, value);
                                            
                                        // Update the execution index
                                        self.execution_index += 1;

                                        //Set the log with result of the op
                                        self.log.set_response_message(entry.ballot.1, result.clone());

                                        //Set the log with the state executed
                                        self.log.set_status(entry.ballot.1, LogEntryState::Executed);

                                        continue;

                                    }
                                    Operation::GET(_key) => {
                                        //ToDo
                                        continue;
                                    }
                                }
                            }
                            Err(_) => {
                                panic!("{}: received err invalid operation request", self.id);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
            
        if message.commit_index == self.commit_index { //All the operations have been committed/executed
            //Change state to normal
            println!("{}: now changing state to normal - all entries committed upto {}", self.id, message.commit_index);
            self.state = NORMAL;

            if message.profile {
                println!("{}: ", self.id);
                //Start the same profile election - change state to election
                self.state = ELECTION;
    
                //Start the election cycle with type profile
                self.start_election_cycle(ElectionType::Profile);
            }

            else {
                return; //normal - can process requests
            }
        }

        else {
            //Remain in the recovery state
            self.state = RECOVERY;
            println!("{}: received a stale state & remaining in RECOVERY - my commit index: {} received state index: {}", self.id, self.commit_index, message.commit_index);
        }

    }
    

    fn start_noler_msg_rcv(&self) {

        let transport = Arc::clone(&self.transport);
        let tx = Arc::clone(&self.tx);

        //let replica_address = self.replica_address;
        //let id = self.id;

        thread::spawn(move || {
            let mut buf = [0; 100000];
            //let mut buf = Vec::with_capacity(1024);
            loop {
                //buf.clear(); // Clear the buffer before each iteration
                //buf.resize(1024, 0); // Resize the buffer to the initial capacity

                match transport.receive_from(&mut buf) {
                    Ok((len, from)) => {
                        //Print the length of the received message
                        //println!("Length of the message received: {:?} bytes", len);

                        // Resize the buffer if the received message is larger than the buffer
                        // if len > buf.len() {
                        //     buf.resize(len, 0);
                        // }
                        //buf.resize(len, 0);

                        let channel = String::from("Network");
                        let message = String::from_utf8_lossy(&buf[..len]).to_string();

                        //println!("{}: received data from {:?}: {:?}: {}", id, from, message, replica_address);
                        let channel_message = ChannelMessage {
                            channel: channel,
                            src: from,
                            message: message,
                        };

                        if let Err(err) = tx.send(channel_message) {
                            eprintln!("Failed to send the Network channel message: {:?}", err);
                            continue;
                        }
                    },
                    Err(err) => {
                        if err.kind() != std::io::ErrorKind::WouldBlock {
                            println!("Failed to receive data: {:?}", err);
                        }
                    }
                }
            }
        });
    }


    pub fn start_noler_replica (&mut self) {
        println!("Starting a Noler Replica with ID: {} and Address: {}", self.id, self.replica_address);

        //Start the leaderInitTimer
        if !self.leader_init_timeout.active() {
            println!("{}: leader init timeout inactive - starting the leader init timeout", self.id);
            let _ = self.leader_init_timeout.start();
        }

        //Start the PollMonitorTimeout
        if !self.poll_monitor_timeout.active() {
            println!("{}: poll monitor timeout inactive - starting the poll monitor timeout", self.id);
            let _ = self.poll_monitor_timeout.start();
        }

        //Start thread for receiving messages
        self.start_noler_msg_rcv();

        let rx = Arc::clone(&self.rx);

        loop {
            if let Ok(msg) = rx.recv() {
                //println!("{}: received message: {:?}", self.id, msg);

                match msg.channel.as_str() {
                    "Network" => {

                        let src = msg.src;
                        let wrapper: Result<MessageWrapper, _> = serde_json::from_str(&msg.message);


                        match wrapper {
                            Ok(wrapper) => {
                                match wrapper.msg_type.as_str() {
                                    "RequestVoteMessage" => {
                                        let request_vote_message: Result<RequestVoteMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);

                                        match request_vote_message {
                                            Ok(request_vote_message) => {
                                                //println!("{}: handling the RequestVoteMessage {:?}", self.id, request_vote_message);
                                                self.handle_request_vote_message(src, request_vote_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize request message: {:?}", err);
                                            }
                                        }  
                                    },
                                    "ResponseVoteMessage" => {
                                        let response_vote_message: Result<ResponseVoteMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match response_vote_message {
                                            Ok(response_vote_message) => {
                                                self.handle_response_vote_message(response_vote_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the response message: {:?}", err);
                                            }
                                        }
                                    },

                                    "ConfigMessage" => {
                                        let config_message: Result<ConfigMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match config_message {
                                            Ok(config_message) => {
                                                //println!("{}: received a {:?}", self.id, config_message);
                                                self.handle_config_message(src, config_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the config message: {:?}", err);
                                            }
                                        }
                                    },

                                    "HeartBeatMessage" => {
                                        let heartbeat_message: Result<HeartBeatMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match heartbeat_message {
                                            Ok(heartbeat_message) => {
                                                //println!("{}: received a HeartBeatMessage from Replica {} with ballot {:?} at {:?}", self.id, heartbeat_message.leader_id, heartbeat_message.ballot, Instant::now());
                                                self.handle_heartbeat_message(src, heartbeat_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the heartbeat message: {:?}", err);
                                            }
                                        }
                                    },

                                    "RequestConfigMessage" => {
                                        let request_config_message: Result<RequestConfigMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match request_config_message {
                                            Ok(request_config_message) => {
                                                println!("{}: received a RequestConfigMessage from replica {} with ballot {:?}", self.id, request_config_message.replica_id, request_config_message.ballot);
                                                self.handle_request_config(request_config_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the request config message: {:?}", err);
                                            }
                                        }
                                    },

                                    "PollLeaderMessage" => {
                                        let poll_leader_message: Result<PollLeaderMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match poll_leader_message {
                                            Ok(poll_leader_message) => {
                                               //println!("{}: received a PollLeaderMessage from Replica {} with term {}", self.id, poll_leader_message.candidate_id, poll_leader_message.propose_term);
                                                self.handle_poll_leader_message(poll_leader_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the poll leader message: {:?}", err);
                                            }
                                        }
                                    },

                                    "PollLeaderOkMessage" => {
                                        let poll_leader_ok_message: Result<PollLeaderOkMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match poll_leader_ok_message {
                                            Ok(poll_leader_ok_message) => {
                                                //println!("{}: received a PollLeaderOkMessage from Replica {} with term {} at {:?}", self.id, poll_leader_ok_message.leader_id, poll_leader_ok_message.propose_term, Instant::now());
                                                self.handle_poll_leader_ok_message(poll_leader_ok_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the poll leader ok message: {:?}", err);
                                            }
                                        }
                                    },


                                    //Request Messages
                                    "RequestMessage" => {
                                        let request_message: Result<RequestMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match request_message {
                                            Ok(request_message) => {
                                                //println!("{}: received a RequestMessage from Replica {} with term {}", self.id, request_message.client_id, request_message.propose_term);
                                                self.handle_request_message(request_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the request message: {:?}", err);
                                            }
                                        }
                                    },

                                    //ProposeMessage
                                    "ProposeMessage" => {
                                        let propose_message: Result<ProposeMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match propose_message {
                                            Ok(propose_message) => {
                                                //println!("{}: received a ProposeMessage from Replica {} with term {}", self.id, propose_message.leader, propose_message.propose_term);
                                                self.handle_propose_message(src, propose_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the propose message: {:?}", err);
                                            }
                                        }
                                    },

                                    //ProposeOkMessage
                                    "ProposeOkMessage" => {
                                        let propose_ok_message: Result<ProposeOkMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match propose_ok_message {
                                            Ok(propose_ok_message) => {
                                                //println!("{}: received a ProposeOkMessage from Replica {} with term {}", self.id, propose_ok_message.leader, propose_ok_message.propose_term);
                                                self.handle_propose_ok(src, propose_ok_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the propose ok message: {:?}", err);
                                            }
                                        }
                                    },

                                    //CommitMessage
                                    "CommitMessage" => {
                                        let commit_message: Result<CommitMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match commit_message {
                                            Ok(commit_message) => {
                                                //println!("{}: received a CommitMessage from Replica {} with term {}", self.id, commit_message.leader, commit_message.propose_term);
                                                self.handle_commit(src, commit_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the commit message: {:?}", err);
                                            }
                                        }
                                    },

                                    //RequestStateMessage
                                    "RequestStateMessage" => {
                                        let request_state_message: Result<RequestStateMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match request_state_message {
                                            Ok(request_state_message) => {
                                                println!("{}: received a RequestStateMessage {:?} from replica {:?} with ballot {:?}", self.id, request_state_message, src, request_state_message.ballot);
                                                self.handle_request_state(src, request_state_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the request state message: {:?}", err);
                                            }
                                        }
                                    },

                                    //LogStateMessage
                                    "LogStateMessage" => {
                                        let log_state_message: Result<LogStateMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);
                                            
                                        match log_state_message {
                                            Ok(log_state_message) => {
                                                println!("{}: received a LogStateMessage with ballot {:?} and commit index {}", self.id, log_state_message.ballot, log_state_message.commit_index);
                                                self.handle_received_state(src, log_state_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the log state message: {:?}", err);
                                            }
                                        }
                                    },

                                    "ResponseProfileMessage" => {
                                        let response_profile_message: Result<ResponseProfileMessage, _> = 
                                            serde_json::from_str(&wrapper.msg_content);

                                        match response_profile_message {
                                            Ok(response_profile_message) => {
                                                //println!("{}: received a ResponseProfileMessage from Replica {} with term {}", self.id, response_profile_message.leader, response_profile_message.propose_term);
                                                self.handle_response_profile_message(response_profile_message);
                                            },

                                            Err(err) => {
                                                println!("Failed to deserialize the response profile message: {:?}", err);
                                            }
                                        }
                                    }



                                    _ => {
                                        println!("{}: received message of unknown Network channel type!", self.id);
                                    }
                                }
                            },
                            Err(err) => {
                                println!("Failed to deserialize wrapper: {:?}", err);
                            }
                        }
                    },

                    "Tx"=> {

                        match msg.message.as_str() {
                            "LeaderInitTimeout" => {
                                self.handle_leader_init_timeout();
                            },

                            "LeaderVoteTimeout" => {
                                self.handle_leader_vote_timeout();
                            },

                            "LeaderProfileVoteTimeout" => {
                                self.handle_leader_profile_vote_timeout();
                            },

                            "LeaderShipVoteTimeout" => {
                                self.handle_leadership_vote_timeout();
                            },

                            "LeaderLeaseTimeout" => {
                                self.handle_leader_lease_timeout();
                            },

                            "PollLeaderTimeout" => {
                                self.handle_poll_leader_timeout();
                            },

                            "HeartBeatTimeout" => {
                                self.handle_heartbeat_timeout();
                            },

                            "PollLeaderTimer" => {
                                self.handle_poll_leader_timer();
                            },

                            "PollMonitorTimeout" => {
                                self.handle_poll_monitor_timeout();
                            },

                            _ => {
                                println!("{}: received message {:?} of unknown Tx channel type!", msg.message, self.id);
                            }
                        }
                    },

                    _ => {
                        println!("{}: received message of unknown channel!", self.id);
                    }
                }

            }
        }

    }
}