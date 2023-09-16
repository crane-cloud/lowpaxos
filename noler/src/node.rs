use std::net::SocketAddr;
use std::time::Instant;
use rand::Rng;
use serde_json;
use std::thread;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::quorum::QuorumSet;
use crate::timeout::Timeout;
use common::utility::wrap_and_serialize;
use kvstore::KVStore;

use crate::constants::*;
use crate::config::{Config, Replica};
use crate::role::Role;
use crate::log::Log;
use crate::message::{ElectionType,
                    *,
                    };
use crate::transport::Transport;
use crate::monitor::{Profile, ProfileMatrix};

pub struct NolerClient {
    client_id: u32,
    client_address: SocketAddr,
    config: Config,
}

impl NolerClient {
    pub fn new(client_id: u32, client_address: SocketAddr, config: Config) -> NolerClient {
        NolerClient {
            client_id: client_id,
            client_address: client_address,
            config: config,
        }
    }

    pub fn start_noler_client(&mut self) {
        println!("{}: starting the noler client", self.client_id);
    }
}


pub struct NolerReplica {
    id: u32,
    replica_address: SocketAddr,
    role: Role,
    state: u8,
    //propose_term: u32,
    //propose_number: u64,
    ballot: (u32, u64),
    voted: (u32, Option<(u32, u64)>), // leader & ballot
    pub leader: Option<u32>, //To be part of the config
    leadership_quorum: QuorumSet<(u32, u64), ResponseVoteMessage>,
    propose_quorum: QuorumSet<(u32, u32), ProposeOkMessage>,
    propose_read_quorum: QuorumSet<(u32, u32), ProposeReadOkMessage>,
    monitor: ProfileMatrix,
    log: Log,
    config: Config,
    transport:  Arc<Transport>,
    data: KVStore,
    commit_index: u64,  
    execution_index: u64,

    leader_init_timeout: Timeout,
    leader_vote_timeout: Timeout,
    leadership_vote_timeout: Timeout,
    leader_lease_timeout: Timeout,
    poll_leader_timeout: Timeout,
    heartbeat_timeout: Timeout,

    poll_leader_timer: Timeout,

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
                message: "LeaderInitTimeout".to_string(),
            }).unwrap();
        }));

        let leader_vote_timeout = Timeout::new(LEADER_VOTE_TIMEOUT, Box::new(move || {
            tx_leader_vote.send(ChannelMessage {
                channel: "Tx".to_string(),
                message: "LeaderVoteTimeout".to_string(),
            }).unwrap();
        }));

        let leadership_vote_timeout = Timeout::new(LEADERSHIP_VOTE_TIMEOUT, Box::new(move || {
            tx_leadership_vote.send(ChannelMessage {
                channel: "Tx".to_string(),
                message: "LeaderShipVoteTimeout".to_string(),
            }).unwrap();
        }));

        let leader_lease_timeout = Timeout::new(LEADER_LEASE_TIMEOUT, Box::new(move || {
            tx_leader_lease.send(ChannelMessage {
                channel: "Tx".to_string(),
                message: "LeaderLeaseTimeout".to_string(),
            }).unwrap();
        }));

        let poll_leader_timeout = Timeout::new(POLL_LEADER_TIMEOUT, Box::new(move || {
            tx_poll_leader.send(ChannelMessage {
                channel: "Tx".to_string(),
                message: "PollLeaderTimeout".to_string(),
            }).unwrap();
        }));

        let heartbeat_timeout = Timeout::new(HEARTBEAT_TIMEOUT, Box::new(move || {
            tx_heartbeat.send(ChannelMessage {
                channel: "Tx".to_string(),
                message: "HeartBeatTimeout".to_string(),
            }).unwrap();
        }));

        let poll_leader_timer = Timeout::new(POLL_LEADER_TIMER, Box::new(move || {
            tx_poll_leader_timer.send(ChannelMessage {
                channel: "Tx".to_string(),
                message: "PollLeaderTimer".to_string(),
            }).unwrap();
        }));

        let mut matrix = ProfileMatrix::new(config.n as usize);
        matrix.set(0, 1, Profile { x: 70}); matrix.set(0, 2, Profile { x: 60}); matrix.set(0, 3, Profile { x: 40}); matrix.set(0, 4, Profile { x: 50});
        matrix.set(1, 0, Profile { x: 75}); matrix.set(1, 2, Profile { x: 50}); matrix.set(1, 3, Profile { x: 75}); matrix.set(1, 4, Profile { x: 60});
        matrix.set(2, 0, Profile { x: 60}); matrix.set(2, 1, Profile { x: 50}); matrix.set(2, 3, Profile { x: 40}); matrix.set(2, 4, Profile { x: 45});
        matrix.set(3, 0, Profile { x: 65}); matrix.set(3, 1, Profile { x: 80}); matrix.set(3, 2, Profile { x: 70}); matrix.set(3, 4, Profile { x: 50});
        matrix.set(4, 0, Profile { x: 60}); matrix.set(4, 1, Profile { x: 55}); matrix.set(4, 2, Profile { x: 70}); matrix.set(4, 3, Profile { x: 30});

        NolerReplica {
            id: id,
            replica_address: replica_address,
            role: Role::new(),
            state: INITIALIZATION,
            ballot: (0, 0),
            voted: (0, None),
            leader: None,
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

        println!("{}: starting an election cycle with type {:?}", self.id, election_type);

        assert!(self.role != Role::Leader, "Only a non-leader can start an election cycle");
        assert!(self.state == ELECTION, "Only a replica in election state can start an election cycle");

        if election_type == ElectionType::Offline {
            assert!(self.role == Role::Candidate || self.role == Role::Witness, "Only a candidate/witness can start offline election cycle");
            assert!(self.poll_leader_timeout.active() || self.poll_leader_timer.active()|| !self.heartbeat_timeout.active(), 
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
                    ballot: (self.ballot.0 + 1, self.ballot.1 + 1),
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
            self.voted = (message.replica_id, Some(message.ballot));
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
            self.voted = (message.replica_id, Some(message.ballot));                

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

        //If the replica has never voted before [possible it is just starting up]
        if self.voted.1 == None {

            //Stop the leader_init_timeout
            self.leader_init_timeout.stop();

            // Check if the request has a fresh ballot
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

        // The replica has voted before
        else if self.voted.1 != None {
            //Check if it is a duplicate request
            if message.ballot == self.voted.1.unwrap() && message.replica_id == self.voted.0 {
                //Provide the same response
                println!("{}: duplicate vote - resending same vote response", self.id);
                self.provide_vote_response(message.ballot, message.replica_address);
                return;
            }

            if message.ballot == self.voted.1.unwrap() {
                match message.replica_role {
                    Role::Leader => {
                        //Ignore the message
                        println!("{}: leader can't restart election with same ballot", self.id);
                        return;
                    },
                    Role::Candidate => {
                        //Confirm the election type is timeout
                        if message.election_type == ElectionType::Timeout {

                            self.proceed_vote_else_response(message.ballot, message);
                        }
                        else {
                            //Ignore the message
                            println!("{}: similar ballots can only be of type election timeout", self.id);
                            return;
                        }
                    },
                    Role::Witness => {
                        //Ignore the message
                        println!("{}: witness role is only after a leader is elected", self.id);
                        return;
                    },
                    Role::Member => {
                    //Confirm the election type is degraded | initial
                        if message.election_type == ElectionType::Degraded {
                            //Update the voted tuple
                            self.voted = (message.replica_id, Some(message.ballot));

                            //Send the ResponseVoteMessage immediately
                            self.provide_vote_response(message.ballot, message.replica_address);

                            return;
                        }
                        else {
                            //Ignore the message
                            println!("{}: similar ballots can only be of type degraded", self.id);
                            return;
                        }
                    },
                }
            }

            //Check if the ballot is fresh
            if self.fresh_ballot(message.ballot) {

                match message.election_type {

                    ElectionType::Profile => {
                        if self.role == Role::Leader {

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
                                self.voted = (message.replica_id, Some(message.ballot));                

                                //Send the ResponseVoteMessage to the source
                                self.provide_vote_response(message.ballot, message.replica_address);

                                //Start the leadervotetimer
                                let _ = self.leader_vote_timeout.start(); //Old leader can reclaim leadership at the end of this timeout

                                return;
                            }

                        }

                        else if self.role == Role::Candidate || self.role == Role::Witness {
                            match message.replica_role {
                                Role::Leader => {
                                    //Ignore the message
                                    println!("{}: leader role can't restart a profile election [just as candidate|witness]", self.id);
                                    return;
                                },

                                Role::Candidate => {
                                    //Process the result
                                    println!("{}: processing the vote request - leader profile detected low at candidate", self.id);

                                    self.proceed_vote_else_response(message.ballot, message);
                                },

                                Role::Witness => {
                                    //Provide the vote result
                                    println!("{}: processing the vote request - leader profile detected low at witness", self.id);

                                    self.proceed_vote_else_response(message.ballot, message);
                                },

                                Role::Member => {
                                    //Ignore the message
                                    println!("{}: member can't start this election type", self.id);
                                    return;
                                },
                            }
                        }

                        else if self.role == Role::Member {
                            //ToDo - for now, ignore the message
                            println!("{}: ignore - will be informed of the election results", self.id);
                            return;
                        }

                    },

                    ElectionType::Offline => {

                        if self.role == Role::Leader {

                            println!("{}: leader is still alive", self.id);

                            //Assert liveness with the ConfigMessage [ToDo - maybe should be confirmed]
                            self.affirm_leadership();

                            return;
                        }

                        else if self.role == Role::Candidate || self.role == Role::Witness {
                            match message.replica_role {
                                Role::Leader => {
                                    //Ignore the message
                                    println!("{}: leader role can't restart an offline election", self.id);
                                    return;
                                },

                                Role::Candidate => {
                                    //PollLeaderTimeout and/or HeartBeatTimeout expired | Process
                                    println!("{}: processing the vote request - leader detected offline", self.id);

                                    self.proceed_vote_else_response(message.ballot, message);
                                },

                                Role::Witness => {
                                    //HeartBeatTimer expired - no candidate elected, witness has started election request
                                    println!("{}: processing the vote request - leader detected offline", self.id);

                                    self.proceed_vote_else_response(message.ballot, message);
                                    
                                },

                                Role::Member => {
                                    //Ignore the message
                                    println!("{}: member can't start this election type", self.id);
                                    return;
                                },
                            }
                        }

                        else if self.role == Role::Member {
                            //ToDo - for now, ignore the message
                            println!("{}: member will wait for election results - may get privileged", self.id);
                            return;
                        }

                    },

                    ElectionType::Degraded => {
                        println!("{}: only profile & offline election types accepted", self.id);
                        return;
                    },
                    
                    ElectionType::Timeout => {
                        println!("{}: only profile & offline election types accepted", self.id);
                        return;
                    },

                    ElectionType::Normal => {
                        println!("{}: only profile & offline election types accepted", self.id);
                        return;
                    },
                    
                }

            }
        }

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

        if let Some(message) = self.leadership_quorum.add_and_check_for_quorum(message.ballot, message.replica_id as i32, message.clone()) {
            //We have quorum for leadership - need to communicate to other replicas

            println!("{}: Quorum check -> {:?}", self.id, message);

            println!("{}: has quorum for leadership", self.id);

            //Update the ballot to +1
            self.ballot = (self.ballot.0 + 1, self.ballot.1 + 1);

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

    fn handle_config_message(&mut self, message: ConfigMessage) {
        println!("{}: received a ConfigMessage from Replica {}", self.id, message.leader_id);

        //Stop leader initialization timeouts
        self.leader_init_timeout.stop();
        self.leader_vote_timeout.stop();
        self.leadership_vote_timeout.stop();

        if self.id == message.leader_id {
            println!("{}: received a ConfigMessage from itself", self.id);
            return;
        }

        if !self.fresh_ballot(message.config.ballot) {
            println!("{}: received a ConfigMessage with a stale ballot", self.id);
            return;
        }

        //Update the replica config
        self.config = message.config.clone();

        //Use the replica config to update some state
        println!("{}: updating ballot to {:?}", self.id, self.config.ballot);

        self.ballot = self.config.ballot;

        self.leader = Some(message.leader_id);

        for replica in self.config.replicas.iter() {
            if replica.id == self.id {
                self.role = replica.role;
            }
        }

        // Start the leader check timers
        match self.role {

            Role::Leader => {
                //Ignore
                println!("{}: ignore - already a leader", self.id);
                return;
            },

            Role::Candidate => {
                //Start the poll leader and heartbeat timeouts
                let _ = self.poll_leader_timeout.reset();
                let _ = self.heartbeat_timeout.reset();
                let _ = self.poll_leader_timer.reset();

                return;
            },

            Role::Witness => {
                //Start the heartbeat timeout
                let _ = self.heartbeat_timeout.reset();

                return;
            },

            Role::Member => {
                //Ignore as the replica must have updated its role at this point
                println!("{}: ignore - already a candidate/witness", self.id);
                return;
            },
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

        assert!(self.ballot == message.ballot, "Only a replica with the same ballot can handle a HeartBeatMessage");
        assert!(self.role != Role::Leader, "Only a non-leader can handle a HeartBeatMessage");
        assert!(self.leader == Some(message.leader_id), "Only accepting replica with my leader info");

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
                // Check the variation in the profiles
                if (p2 - p1) > 60 {
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
                if let Some(leader) = self.config.replicas.iter().find(|r| r.id == self.leader.unwrap()) { leader.replica_address.clone() }
                else { self.replica_address.clone() }
            };
            
            let _ = self.transport.send(
                &leader_address,
                &mut serialized_plm.as_bytes(),
            );

            return;
        }
    }


    //Request Processing Handlers
    fn handle_request_message(&mut self, message: RequestMessage) {
        //Need to get the message type and act accordingly


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
                    Ok((len, _from)) => {

                        let channel = String::from("Network");
                        let message = String::from_utf8_lossy(&buf[..len]).to_string();

                        //println!("{}: received data from {:?}: {:?}: {}", id, from, message, replica_address);

                        tx.send(
                            ChannelMessage {
                                channel: channel,
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
                                                self.handle_config_message(config_message);
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

