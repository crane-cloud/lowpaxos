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

use crate::{constants::*, message};
use crate::config::{Config, Replica};
use crate::role::Role;
use crate::log::Log;
use crate::log::{LogEntryState, LogEntry};
use crate::message::{ElectionType,
                    *,
                    };
use crate::transport::Transport;
use crate::monitor::{Profile, ProfileMatrix};

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
    propose_read_quorum: QuorumSet<(u32, u64), ProposeReadOkMessage>,
    pub monitor: ProfileMatrix,
    log: Log,
    pub config: Config,
    pub transport:  Arc<Transport>,
    data: KVStore,
    commit_index: u64,  
    execution_index: u64,

    pub leader_init_timeout: Timeout,
    pub leader_vote_timeout: Timeout,
    pub leadership_vote_timeout: Timeout,
    pub leader_lease_timeout: Timeout,
    pub poll_leader_timeout: Timeout,
    pub heartbeat_timeout: Timeout,

    pub poll_leader_timer: Timeout,

    tx: Arc<Sender<ChannelMessage>>,
    rx: Arc<Receiver<ChannelMessage>>,

}

impl NolerReplica {
    pub fn new(id: u32, replica_address: SocketAddr, config: Config, transport: Transport) -> NolerReplica {

        let (tx, rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) = unbounded();

        let init_timer = rand::thread_rng().gen_range(1..5);

        let tx_leader_init = tx.clone();
        let tx_leader_vote = tx.clone();
        let tx_leadership_vote = tx.clone();
        let tx_leader_lease = tx.clone();
        let tx_poll_leader = tx.clone();
        let tx_heartbeat = tx.clone();

        let tx_poll_leader_timer = tx.clone();

        let leader_init_timeout = Timeout::new(init_timer, Box::new(move || {
            tx_leader_init.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderInitTimeout".to_string(),
            }).unwrap();
        }));

        let leader_vote_timeout = Timeout::new(LEADER_VOTE_TIMEOUT, Box::new(move || {
            tx_leader_vote.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderVoteTimeout".to_string(),
            }).unwrap();
        }));

        let leadership_vote_timeout = Timeout::new(LEADERSHIP_VOTE_TIMEOUT, Box::new(move || {
            tx_leadership_vote.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderShipVoteTimeout".to_string(),
            }).unwrap();
        }));

        let leader_lease_timeout = Timeout::new(LEADER_LEASE_TIMEOUT, Box::new(move || {
            tx_leader_lease.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "LeaderLeaseTimeout".to_string(),
            }).unwrap();
        }));

        let poll_leader_timeout = Timeout::new(POLL_LEADER_TIMEOUT, Box::new(move || {
            tx_poll_leader.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "PollLeaderTimeout".to_string(),
            }).unwrap();
        }));

        let heartbeat_timeout = Timeout::new(HEARTBEAT_TIMEOUT, Box::new(move || {
            tx_heartbeat.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "HeartBeatTimeout".to_string(),
            }).unwrap();
        }));

        let poll_leader_timer = Timeout::new(POLL_LEADER_TIMER, Box::new(move || {
            tx_poll_leader_timer.send(ChannelMessage {
                channel: "Tx".to_string(),
                src: replica_address,
                message: "PollLeaderTimer".to_string(),
            }).unwrap();
        }));

        let mut matrix = ProfileMatrix::new(config.n as usize);

        let values = vec![
            vec![74, 65, 66, 53],
            vec![74, 89, 83, 69],
            vec![65, 89, 72, 100],
            vec![66, 83, 72, 69],
            vec![53, 69, 100, 73],
        ];

        for i in 0..values.len() {
            for j in 0..values[i].len() {
                let _ = matrix.set(i, j, Profile { x: values[i][j] });
            }
        }

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
            propose_read_quorum: QuorumSet::new((config.f as i32)/2 + 1),
            monitor: matrix,
            log: Log::new(),
            config: config,
            transport: Arc::new(transport),
            data: KVStore::new(),
            commit_index: 0,
            execution_index: 0,

            leader_init_timeout,
            leader_vote_timeout,
            leadership_vote_timeout,
            leader_lease_timeout,
            poll_leader_timeout,
            heartbeat_timeout,

            poll_leader_timer,

            tx: Arc::new(tx),
            rx: Arc::new(rx),

        }
    }

    // A replica starts an election cycle with an election type dependant on the timer received.
    fn start_election_cycle (&mut self, election_type: ElectionType) {

        let ballot = (self.ballot.0 + 1, self.ballot.1 + 1);

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
                        if let Some(profile) = self.monitor.get((self.id - 1) as usize, (replica.id - 1) as usize) { profile.get_x() }
                        else { 0 }
                    },
                    election_type: election_type,
                };

                println!("{}: sending a RequestVoteMessage to Replica {}:{} with profile {}",
                    self.id, 
                    replica.id, 
                    replica.replica_address,
                    request_vote_message.replica_profile
                );

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
        if ballot.0 > self.ballot.0 && ballot.1 > self.ballot.1 {
            return true;
        }
        else {
            return false;
        }
    }

    //Function to determine if next ballot
    fn next_ballot (&mut self, ballot: (u32, u64)) -> bool {
        if ballot.0 == self.ballot.0 + 1 && ballot.1 == self.ballot.1 + 1 {
            return true;
        }
        else {
            return false;
        }
    }

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
        if self.profile_vote_result({
            if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.replica_id - 1) as usize) { profile.get_x() }
            else { 0 }
        }, 
            message.replica_profile) 
            {

            println!("{}: vote - Replica {}:{} has a better profile than Replica {}:{}", self.id,
                message.replica_id, 
                {
                    if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.replica_id - 1) as usize) { profile.get_x() }
                    else { 0 }
                },
                self.id,
                message.replica_profile, 
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
                message.replica_profile,
                message.replica_id,
                {
                    if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.replica_id - 1) as usize) { profile.get_x() }
                    else { 0 }
                },
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

    fn handle_request_vote_message (&mut self, message: RequestVoteMessage) {

        println!("{}: received a RequestVoteMessage from Replica {} with profile {}", self.id, message.replica_id, message.replica_profile);

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

            //Check if the ballot iss the same as voted ballot
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

                ElectionType::Profile => {
                    match self.role {
                        Role::Leader => {
                            if self.profile_vote_result(message.replica_profile, 
                                {
                                    if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.replica_id - 1) as usize) { profile.get_x() }
                                    else { 0 }
                                }
                            ){
                                // Leader profile is still better than source profile
                                println!("{}: leader profile still good - affirming leadership", self.id);

                                self.affirm_leadership();
                                
                                return;
                            }

                            else {
                                // Leader profile has degraded - no need to lead anymore
                                // Change role to candidate & status to election
                                println!("{}: leader profile detected low - changing the role to Candidate", self.id);

                                self.role = Role::Candidate;
                                self.state = ELECTION;

                                //Update the voted tuple
                                self.voted = Some((message.replica_id, message.ballot, message.election_type));                

                                //Send the ResponseVoteMessage to the source
                                self.provide_vote_response(message.ballot, message.replica_address);

                                //Start the leadervotetimer
                                let _ = self.leader_vote_timeout.start(); //Old leader can reclaim leadership at the end of this timeout

                                return;
                            }
                        }

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
                            self.affirm_leadership();
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

        if self.ballot != ((message.ballot.0 - 1), (message.ballot.1 - 1)) {
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
            self.ballot = (self.ballot.0 + 1, self.ballot.1 + 1);

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
                    if let Some(profile) = self.monitor.get((self.id - 1) as usize, (replica.id - 1) as usize) { profile.get_x() }
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

    fn handle_config_message(&mut self, src: SocketAddr, message: ConfigMessage) {
        println!("{}: received a ConfigMessage from Replica {}", self.id, message.leader_id);

        //Stop leader initialization timeouts
        self.leader_init_timeout.stop();
        self.leader_vote_timeout.stop();
        self.leadership_vote_timeout.stop();

        if self.id == message.leader_id || self.role == Role::Leader {
            println!("{}: received a ConfigMessage from itself", self.id);
            return;
        }

        if !self.fresh_ballot(message.config.ballot) {
            println!("{}: received a ConfigMessage with a stale ballot ", self.id);
            return;
        }

        //Update the replica config
        self.config = message.config.clone();

        //Use the replica config to update some state
        println!("{}: updating ballot to {:?}", self.id, self.config.ballot);

        self.ballot = self.config.ballot;

        self.leader = Some((message.leader_id, self.config.ballot));

        //Update the role - but need to check the current role on the replica

        for replica in self.config.replicas.iter() {
            if replica.id == self.id {
                match self.role {
                    Role::Witness => {
                        if replica.role == Role::Candidate {
                            //Change the role to candidate
                            println!("{}: changing the role to Candidate - request state", self.id);
                            self.role = replica.role;
                            
                            //Change status to recovery
                            self.state = RECOVERY;

                            //Send the RequestStateMessage to the leader
                            let request_state_message = RequestStateMessage {
                                ballot: self.ballot,
                                commit_index: self.commit_index,
                            };

                            //Serialize the RequestStateMessage with meta type set
                            let serialized_rsm = wrap_and_serialize(
                                "RequestStateMessage", 
                                serde_json::to_string(&request_state_message).unwrap()
                            );

                            //Send the RequestStateMessage to the leader
                            println!("{}: sending a RequestStateMessage to Replica {} with ballot {:?}", self.id, message.leader_id, self.ballot);

                            let _ = self.transport.send(
                                &src,
                                &mut serialized_rsm.as_bytes(),
                            );

                            //Start the poll leader and heartbeat timeouts
                            let _ = self.poll_leader_timeout.reset();
                            let _ = self.heartbeat_timeout.reset();
                            let _ = self.poll_leader_timer.reset();
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

                            //Change status to normal
                            self.state = NORMAL;

                            //Start the heartbeat timeout
                            let _ = self.heartbeat_timeout.reset();
                        }

                        else {
                            //Still a candidate
                            println!("{}: still a candidate - reset the timers", self.id);
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
                        //Update the role to the replica role
                        println!("{}: changing the role to {:?}", self.id, replica.role);
                        self.role = replica.role;

                        //match on the role
                        match self.role {
                            Role::Witness => {
                                //Change the state to normal
                                self.state = NORMAL;
                    
                                //Start the heartbeat timeout
                                let _ = self.heartbeat_timeout.reset();
                            }

                            Role::Candidate => {
                                //Change the state to normal
                                self.state = NORMAL;

                                //Start the poll leader and heartbeat timeouts
                                let _ = self.poll_leader_timeout.reset();
                                let _ = self.heartbeat_timeout.reset();
                                let _ = self.poll_leader_timer.reset();
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
    fn affirm_leadership (&mut self) {
        //Increment the term to invalidate other vote requests
        self.ballot = (self.ballot.0 + 1, self.ballot.1 + 1);

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
    }

    fn handle_heartbeat_message(&mut self, message: HeartBeatMessage){

        //assert!(self.ballot == message.ballot, "Only a replica with the same ballot can handle a HeartBeatMessage");
        assert!(self.role != Role::Leader, "Only a non-leader can handle a HeartBeatMessage");
        assert!(self.leader.unwrap().0 == message.leader_id, "Only accepting replica with my leader info");

        //Ignore if the heartbeat is from itself
        if self.id == message.leader_id {
            println!("{}: received a HeartBeatMessage from itself", self.id);
            return;
        }

        //Ignore if the heartbeat is from a different term
        if self.ballot != message.ballot {
            println!("{}: received a HeartBeatMessage with a different ballot", self.id); //ToDo - determine if fresh ballot!
            return;
        }

        if self.ballot == message.ballot {
            if self.role == Role::Candidate {

                println!("{}: resetting the HB timers", self.id);
                //Reset the pollleader timer
                let _ = self.poll_leader_timer.reset();
                //Reset the heartbeat timeout
                let _ = self.heartbeat_timeout.reset();
                //Reset the poll leader timeout
                let _ = self.poll_leader_timeout.reset();

                return;
            }

            else if self.role ==Role::Witness {
                println!("{}: resetting the HB timer", self.id);
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
                println!("{}: sending a RequestConfigMessage to Replica {} with ballot {:?}", self.id, message.leader_id, self.ballot);

                let _ = self.transport.send(
                    &message.leader_address,
                    &mut serialized_rcm.as_bytes(),
                );

            }
        }
    }

    fn handle_poll_leader_message(&mut self, message:PollLeaderMessage) {
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
                        if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.candidate_id - 1) as usize) { profile.get_x() }
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
                    if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.leader_id - 1) as usize) { profile.get_x() }
                    else { 0 }
                };

                println!("{}: p2 = {}, p1 = {}", self.id, p1, p2);

                if p2 > p1 {
                    //Check the variation in the profiles

                    if (p2 - p1) >= 60 {
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
                if let Some(profile) = self.monitor.get((self.id - 1) as usize, (message.replica_id - 1) as usize) { profile.get_x() }
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
                            if let Some(profile) = self.monitor.get((self.id - 1) as usize, (replica.id - 1) as usize) { profile.get_x() }
                            else { 0 }
                        },
                    };

                    //Serialize the HeartBeatMessage with meta type set
                    let serialized_hbm = wrap_and_serialize(
                        "HeartBeatMessage", 
                        serde_json::to_string(&heartbeat_message).unwrap()
                    );

                    //Send the HeartBeatMessage to a replica
                    println!("{}: sending a HeartBeatMessage to Replica {} with ballot {:?}", self.id, replica.id, self.ballot);
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
        
        //println!("{}: received PollLeaderTimer - check on the leader at {:?}", self.id, Instant::now());
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
            //println!("{}: sending a PollLeaderMessage to Replica {} with term {} at {:?}", self.id, self.leader.unwrap(), self.propose_term, Instant::now());

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

    fn perform_liveness_check(&mut self) {
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

        if self.role == Role::Leader && self.state == NORMAL {
            let c_id = message.client_id;
            let c_req_id = message.request_id;

            let op_result:Result<Operation<String, String>, _> = bincode::deserialize(&message.operation);

            // let op = String::from_utf8_lossy(&message.operation);
            // let parts: Vec<&str> = op.split_whitespace().collect();

            match op_result {
                Ok(msg) => {
                    match msg {
                        Operation::SET(key, value) => {
                            println!("{}: received a SET request from client {} with request id {} at {}", self.id, c_id, c_req_id, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
                            println!("{}: SET request key = {}, value = {}", self.id, key, value);

                            //Start the Paxos-related logic

                            //Determine if the request is in the log
                            let request_entry = self.log.find_by_ballot_request_id(self.ballot, c_req_id); //Handle locally?

                            if request_entry.is_some() {
                                println!("{}: request already in the log", self.id);
        
                                let entry_state = request_entry.unwrap().state;
        
                                //Check the status of the entry
                                match entry_state {
                                    LogEntryState::Propose => {
                                        //Resend the ProposeMessage to the peers
        
                                        let propose_message = ProposeMessage {
                                            //leader,
                                            ballot: request_entry.unwrap().ballot,
                                            request: request_entry.unwrap().request.clone(),
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
        
                                    LogEntryState::Committed => {
                                        //Get the reply from the log
                                        let reply = request_entry.unwrap().response.clone().unwrap();

                                        // //Send the reply to the client
                                        // println!("{}: sending a ResponseMessage to Client {} with request id {}", self.id, c_id, c_req_id);

                                        // let _ = self.transport.send(
                                        //     &c_id,
                                        //     &reply.to_bytes().unwrap(),
                                        // );
        
                                        //Serialize the ResponseMessage with meta type set
                                        let serialized_rm = wrap_and_serialize(
                                            "ResponseMessage", 
                                            serde_json::to_string(&reply).unwrap()
                                        );
        
                                        //Send the ResponseMessage to the client
                                        println!("{}: sending a ResponseMessage to Client {} with request id {}", self.id, c_id, c_req_id);
        
                                        let _ = self.transport.send(
                                            &c_id,
                                            &mut serialized_rm.as_bytes(),
                                        );
                                    }
        
                                    LogEntryState::Executed => {
                                        //Get the reply from the log
                                        let reply = request_entry.unwrap().response.clone().unwrap();

                                        //Send the reply to the client
                                        println!("{}: sending a ResponseMessage to Client {} with request id {}", self.id, c_id, c_req_id);

                                        let _ = self.transport.send(
                                            &c_id,
                                            &reply.to_bytes().unwrap(),
                                        );
        
                                        //Serialize the ResponseMessage with meta type set
                                        let serialized_rm = wrap_and_serialize(
                                            "ResponseMessage", 
                                            serde_json::to_string(&reply).unwrap()
                                        );
        
                                        //Send the ResponseMessage to the client
                                        println!("{}: sending a ResponseMessage to Client {} with request id {}", self.id, c_id, c_req_id);
        
                                        let _ = self.transport.send(
                                            &c_id,
                                            &mut serialized_rm.as_bytes(),
                                        );
                                    }
        
                                    _ => {}
                                }
                            }
        
                            else {
                                log::info!("{}: adding request to the log", self.id);

                                self.last_op += 1;

                                let ballot = (self.ballot.0, self.last_op);

                                //self.last_op += 1;
        
                                //self.log.append(self.ballot, message.clone(), None, LogEntryState::Propose);
                                self.log.append(ballot, message.clone(), None, LogEntryState::Propose);
        
                                //Send the ProposeMessage to the peers
                                let propose_message = ProposeMessage {
                                    //leader,
                                    ballot: ballot,
                                    //opnum: self.last_op,
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

        else {
            //Forward Request to the leader
            if self.leader.is_some() && self.leader.unwrap().0 != self.id { //Only forward requests to a non-leader
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

        if message.ballot < self.ballot {
            println!("{}: received a stale ProposeMessage - last_op", self.id);
            return;
        }

        if message.ballot.1 < self.last_op {
            println!("{}: received a stale ProposeMessage - last_op", self.id);
            return;
        }

        if self.role == Role::Leader {
            println!("{}: ignore as this is done by non-leader nodes", self.id);
            return;
        }

        if self.state != NORMAL {
            println!("{}: ignore as this is done only in the normal state", self.id);
            return;
        }

        else {

            //Check if the request is in the log
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
                        println!("{}: sending a ProposeOkMessage to Replica {} with ballot {:?}", self.id, src, message.ballot);

                        let _ = self.transport.send(
                            &src,
                            &mut serialized_pom.as_bytes(),
                        );
                    }
                    _ => {}
                }
            }

            else {

                //ToDo: gaps

                if message.ballot.1 == (self.last_op + 1){
                    //Update the last_op
                    self.last_op += 1;

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

                else {
                    //Request for state from the leader

                    let request_state_message = RequestStateMessage {
                        ballot: self.ballot,
                        commit_index: self.commit_index, //last_op
                    };

                    //Serialize the RequestStateMessage with meta type set
                    let serialized_rsm = wrap_and_serialize(
                        "RequestStateMessage", 
                        serde_json::to_string(&request_state_message).unwrap()
                    );

                    //Send the RequestStateMessage to the leader
                    println!("{}: sending a RequestStateMessage to Replica {} with ballot {:?}", self.id, src, self.ballot);
                    let _ = self.transport.send(
                        &src,
                        &mut serialized_rsm.as_bytes(),
                    );
                }
            }
        }
    }

    fn handle_propose_ok(&mut self, src: SocketAddr, message: ProposeOkMessage) {

        if self.role == Role::Leader && self.state == NORMAL {
            
            //if message.ballot == self.ballot {
                println!("{}: received a ProposeOkMessage from Replica {} with ballot {:?}", self.id, src, message.ballot);
                
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
    
                if let Some(entry) =  self.log.find(ballot.1) {
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
            //}

            //Print the log & data
            //println!("{}: log is {:?}", self.id, self.log);
            //println!("{}: data is {:?}", self.id, self.data);


        } else {
            println!("{}: ignore as this is done only at the leader", self.id);
        }
    }

    fn handle_commit(&mut self, src: SocketAddr, message: CommitMessage) {
        if self.role == Role::Leader || self.role == Role::Member {
            println!("{}: ignore as this is done by non-leader/member nodes", self.id);
            return;
        }

        if self.state != NORMAL {
            println!("{}: ignore as this is done only in the normal state", self.id);
            return;
        }
        
        if message.ballot >= self.ballot {
            println!("{}: received a CommitMessage from Replica {} with ballot {:?}", self.id, src, message.ballot);

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
        else if message.ballot > self.ballot {

            //Change state to recovery
            self.state = RECOVERY;

            //Request for the log from the leader
            let request_state_message = RequestStateMessage {
               ballot: self.ballot,
               commit_index: self.commit_index,
            };

            //Serialize the RequestStateMessage with meta type set
            let serialized_rsm = wrap_and_serialize(
                "RequestStateMessage", 
                serde_json::to_string(&request_state_message).unwrap()
            );

            //Send the RequestStateMessage to the leader
            println!("{}: sending a RequestStateMessage to Replica {} with ballot {:?}", self.id, src, message.ballot);
            let _ = self.transport.send(
                &src,
                &mut serialized_rsm.as_bytes(),
            );
        }

        else {
            println!("{}: ignore as the commit message is stale", self.id);
            return;
        }

        //Print the log & data
        //println!("{}: log is {:?}", self.id, self.log);
        //println!("{}: data is {:?}", self.id, self.data);
    }
    

    fn handle_request_state (&mut self, src: SocketAddr, message: RequestStateMessage) {
        if self.role != Role::Leader || self.state != NORMAL {
            println!("{}: ignore as this is done by normal leader node", self.id);
            return;
        }

        if message.ballot >= self.ballot {
            println!("{}: ignore as the request state message is future", self.id); //Stale leader

            //Change state to recovery //ToDo - who is the leader?
            self.state = CONFIGURATION;

            return;
        }

        else {

            //Retrieve the log from message.commit_index to our current commit_index
            let log = self.log.get_entries(message.commit_index, self.commit_index);

            //Serialize the log
            let log = serde_json::to_vec(&log).unwrap();

            //Send the state to the replica
            let state_message = LogStateMessage {
                ballot: self.ballot,
                commit_index: self.commit_index,
                log: log,
            };

            //Serialize the StateMessage with meta type set
            let serialized_sm = wrap_and_serialize(
                "StateMessage", 
                serde_json::to_string(&state_message).unwrap()
            );

            //Send the StateMessage to the replica
            println!("{}: sending a StateMessage to Replica {} with ballot {:?}", self.id, src, message.ballot);
            let _ = self.transport.send(
                &src,
                &mut serialized_sm.as_bytes(),
            );
        }
    }

    fn handle_received_state(&mut self, message:LogStateMessage) {
        if self.state != RECOVERY {
            println!("{}: ignore as this is done in recovery state", self.id);
            return;
        }

        if message.ballot < self.ballot {
            println!("{}: ignore as the state message is stale", self.id);
            return;
        }

        else {
            //Retrieve the log entries from the message
            let log_entries: Vec<LogEntry> = serde_json::from_slice(&message.log).unwrap();

            for entry in log_entries.iter() {
                //Check if the entry is already in the log
                let request_entry = self.log.find(entry.ballot.1);

                if request_entry.is_some() {
                    continue;
                }

                else {
                    //Update the commit index
                    self.commit_index = entry.ballot.1;

                    //Add the entry to the log with status committed
                    self.log.append(entry.ballot, entry.request.clone(), entry.response.clone(), LogEntryState::Committed);

                    //Match the node role
                    match self.role {
                        Role::Candidate => {
                            //Determine the type of operation
                            let operation = entry.request.operation.clone();
                            let op = String::from_utf8_lossy(&operation);
                            let parts: Vec<&str> = op.split_whitespace().collect();

                            match parts[0] {
                                "SET" => {
                                    // Execute the operation on the kvstore
                                    let result = self.data.set(parts[1].to_string(), parts[2].to_string());

                                    // Update the execution index
                                    self.execution_index = self.commit_index;

                                    //Set the log with result of the op
                                    self.log.set_response_message(entry.ballot.1, result.clone());

                                    //Set the log with the state executed
                                    self.log.set_status(entry.ballot.1, LogEntryState::Executed);
                                }
                                _ => {
                                    println!("{}: received invalid request", self.id);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if message.commit_index == self.commit_index { //All the operations have been committed/executed
            //Change state to normal
            self.state = NORMAL;
        }

        else {
            //Remain in the recovery state
            self.state = RECOVERY;
            panic!("{}: received a stale state", self.id);
        }
    }

    fn start_noler_msg_rcv(&self) {

        let transport = Arc::clone(&self.transport);
        let tx = Arc::clone(&self.tx);

        //let replica_address = self.replica_address;
        //let id = self.id;

        thread::spawn(move || {
            let mut buf = [0; 1024];
            loop {
                match transport.receive_from(&mut buf) {
                    Ok((len, from)) => {

                        let channel = String::from("Network");
                        let message = String::from_utf8_lossy(&buf[..len]).to_string();

                        //println!("{}: received data from {:?}: {:?}: {}", id, from, message, replica_address);

                        tx.send(
                            ChannelMessage {
                                channel: channel,
                                src: from,
                                message: message,
                            }
                        ).unwrap()
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
                                                self.handle_request_vote_message(request_vote_message);
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
                                                println!("{}: received a HeartBeatMessage from Replica {} with ballot {:?} at {:?}", self.id, heartbeat_message.leader_id, heartbeat_message.ballot, Instant::now());
                                                self.handle_heartbeat_message(heartbeat_message);
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
                                                println!("{}: received a RequestConfigMessage from Replica {} with ballot {:?}", self.id, request_config_message.replica_id, request_config_message.ballot);
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
                                                //println!("{}: received a RequestStateMessage from Replica {} with term {}", self.id, request_state_message.leader, request_state_message.propose_term);
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
                                                //println!("{}: received a LogStateMessage from Replica {} with term {}", self.id, log_state_message.leader, log_state_message.propose_term);
                                                self.handle_received_state(log_state_message);
                                            },
                                            Err(err) => {
                                                println!("Failed to deserialize the log state message: {:?}", err);
                                            }
                                        }
                                    },



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

                            _ => {
                                println!("{}: received message of unknown Tx channel type!", self.id);
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