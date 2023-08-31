use std::net::SocketAddr;
// use std::time::{Duration, Instant};
use rand::Rng;
use serde_json;
use std::thread;
//use tokio::spawn;
//use tokio::sync::mpsc;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};

use crate::quorum::QuorumSet;
use crate::timeout::Timeout;
use common::utility::wrap_and_serialize;
use kvstore::KVStore;

use crate::constants::*;
use crate::config::{Config, Replica, ReplicaConfig};
use crate::role::Role;
use crate::log::Log;
use crate::message::{ElectionType,
                    *,
                    };
use crate::transport::Transport;
use crate::monitor::ProfileMatrix;

struct Client {
    client_id: SocketAddr,
    config: Config,
}

type TimeoutCallback = fn(&mut NolerReplica);

pub struct NolerReplica {
    id: u32,
    replica_address: SocketAddr,
    role: Role,
    state: u8,
    propose_term: u32,
    propose_number: u64,
    voted: (u32, Option<u32>),
    leader: Option<u32>, //To be part of the config
    leadership_quorum: QuorumSet<(u32, u32), ResponseVoteMessage>,
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

    tx: Sender<String>,
    rx: Receiver<String>,

    // on_leader_init_timeout: Option<TimeoutCallback>,
    // on_leader_vote_timeout: Option<TimeoutCallback>,
    // on_leadership_vote_timeout: Option<TimeoutCallback>,
    // on_leader_lease_timeout: Option<TimeoutCallback>,

}

impl NolerReplica {
    pub fn new(id: u32, replica_address: SocketAddr, config: Config, transport: Transport, tx: Sender<String>, rx: Receiver<String>) -> NolerReplica {

        let init_timer = rand::thread_rng().gen_range(2500..5000);

        let tx_leader_init = tx.clone();
        let tx_leader_vote = tx.clone();
        let tx_leadership_vote = tx.clone();
        let tx_leader_lease = tx.clone();

        let leader_init_timeout = Timeout::new(init_timer, Box::new(move || {
            tx_leader_init.send("leader_init_timeout".to_string()).unwrap();
        }));

        let leader_vote_timeout = Timeout::new(LEADER_VOTE_TIMEOUT, Box::new(move || {
            tx_leader_vote.send("leader_vote_timeout".to_string()).unwrap();
        }));

        let leadership_vote_timeout = Timeout::new(LEADERSHIP_VOTE_TIMEOUT, Box::new(move || {
            tx_leadership_vote.send("leadership_vote_timeout".to_string()).unwrap();
        }));

        let leader_lease_timeout = Timeout::new(LEADER_LEASE_TIMEOUT, Box::new(move || {
            tx_leader_lease.send("leader_lease_timeout".to_string()).unwrap();
        }));


        //let transport = Transport::new(replica_address).expect("Transport initialization failed");

        NolerReplica {
            id: id,
            replica_address: replica_address,
            role: Role::new(),
            state: INITIALIZATION,
            propose_term: 0,
            propose_number: 0,
            voted: (0, None),
            leader: None,
            leadership_quorum: QuorumSet::new(config.f as i32),
            propose_quorum: QuorumSet::new(config.f as i32),
            propose_read_quorum: QuorumSet::new((config.f as i32)/2 + 1),
            monitor: ProfileMatrix::new(config.n as usize),
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

            tx: tx,
            rx: rx,

        }
    }


    fn start_election_cycle (&mut self, election_type: ElectionType) {

        println!("{}: Starting an election cycle with type {:?}", self.id, election_type);


        if self.propose_term == 0 {

            self.propose_term += 1;

            //Change state to ELECTION
            self.state = ELECTION;

            //Create a RequestVoteMessage with term + 1 & type = Normal
            let request_vote_message = RequestVoteMessage {
                replica_id: self.id,
                replica_address: self.replica_address,
                replica_role: self.role,
                propose_term: self.propose_term,
                replica_profile: self.monitor.get_profile_x(self.id as usize),
                election_type: election_type,
            };

            //Serialize the RequestVoteMessage with meta type set
            let serialized_rvm = wrap_and_serialize(
                "RequestVoteMessage", 
                serde_json::to_string(&request_vote_message).unwrap()
            );

            //Broadcast the RequestVoteMessage to all replicas

            println!("Broadcasting the RequestVoteMessage to all replicas");

            let m = self.transport.broadcast(
                 &self.config,
                 &mut serialized_rvm.as_bytes(),
            );

            println!("The result of the broadcast is {:?}", m);
            return;
        }
        else {
            println!("Handle other election types");
        }
    }

    fn create_replica_config (&mut self) -> ReplicaConfig {
        ReplicaConfig {
            propose_term: self.propose_term,
            replica: Replica::new(self.id, self.replica_address),
        }
    }

    fn provide_replica_config (&mut self, message: RequestReplicaConfigMessage) {

    }

    // Function to compare replica profiles
    fn profile_vote_result (&mut self, source_profile: Option<f32>, dest_profile: Option<f32>) -> bool {
        if source_profile >= dest_profile {
            return true;
        }
        else {
            return false;
        }
    }

    // Function processes the profile in the RequestVoteMessage
    fn process_profile_vote (&mut self, message: RequestVoteMessage) {
        //Check if the replica's profile is better than the source's profile
        if self.profile_vote_result(self.monitor.get_profile_y(self.id as usize), 
            message.replica_profile) 
            {

            println!("{}: vote - Replica {} has a better profile than Replica {}", self.id, message.replica_id, self.id);
            //Update the voted tuple
            self.voted = (message.propose_term, Some(message.replica_id));                

            //Send the ResponseVoteMessage to the source
            self.provide_vote_response(message.propose_term, message.replica_address);

            //Start the leadershipvotetimer
            ////let _ = self.leadership_vote_timeout.start();
            //start a degraded election type start_election_cycle(election_type: degraded)
            //return;

        }
        else {
            println!("{}: noVote - Replica {} has a better profile than Replica {}", self.id, self.id, message.replica_id);

            println!("{}: changing the role to Candidate", self.id);
            self.role = Role::Candidate;

            //Start the leadervotetimer

            println!("{}: starting the leader vote timeout", self.id);

            //Start the leader vote timeout
            self.leader_vote_timeout.start().unwrap();

            // self.set_leader_vote_timeout_callback(Self::on_leader_vote_timeout);
            // self.leader_vote_timeout.start(Duration::from_millis(LEADER_VOTE_TIMEOUT));

            //Start the election cycle type start_election_cycle(election_type: timeout)
            //return; //ignore
        }
    }

    fn provide_vote_response (&mut self, propose_term: u32, destination: SocketAddr) {
        //Create the ResonseVoteMessage
        let response_vote_message = ResponseVoteMessage {
            replica_id: self.id,
            replica_address: self.replica_address,
            propose_term: propose_term,
            propose_number: self.propose_number,
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
        return;
    }
    
    fn handle_request_vote_message (&mut self, message: RequestVoteMessage) {

        //Stop the leader_init_timeout
        self.leader_init_timeout.stop();

        println!("{}: received a RequestVoteMessage from Replica {}", self.id, message.replica_id);

        //Ignore if the replica is the source of the message
        if self.replica_address == message.replica_address {
            println!("{}: received a RequestVoteMessage from itself", self.id);
            return;
        }

        //If the replica has never voted before
        if self.voted.1 == None || self.propose_term < message.propose_term {
            println!("{}: has never voted before", self.id);

            //Change the state to election
            self.state = ELECTION;

            //Process the request
            self.process_profile_vote(message);

            println!("{}: yield back control to the main loop", self.id);

            return;
        }

        // The leader has voted before
        else if self.voted.1 != None {
            //If duplicate message, provide the same responses
            if self.voted.0 == message.propose_term && self.voted.1 == Some(message.replica_id) {
                println!("Duplicate vote - resending same vote response");
                self.provide_vote_response(message.propose_term, message.replica_address);

                return;
            }

            if message.propose_term < self.voted.0 || message.propose_term < self.propose_term {
                println!("Stale term - ignoring vote request");
                //Ignore the message
                return;
            }

            if message.propose_term == self.voted.0 || message.propose_term == self.propose_term {
                match message.replica_role {
                    Role::Leader => {
                        //Ignore the message
                        println!("Leader can't restart election with same term");
                        return;
                    },
                    Role::Candidate => {
                        //Confirm the election type is timeout
                        if message.election_type == ElectionType::Timeout {
                            //Process the vote request
                            self.process_profile_vote(message);
                        }
                        else {
                            //Ignore the message
                            println!("Similar election terms can only be of type timeout");
                            return;
                        }
                    },
                    Role::Witness => {
                        //Ignore the message
                        println!("Witness role is only after a leader is elected");
                        return;
                    },
                    Role::Member => {
                    //Confirm the election type is degraded
                        if message.election_type == ElectionType::Degraded {
                            //Update the voted tuple
                            self.voted = (message.propose_term, Some(message.replica_id));

                            //Send the ResponseVoteMessage immediately
                            self.provide_vote_response(message.propose_term, message.replica_address);

                            return;
                        }
                        else {
                            //Ignore the message
                            println!("Similar election terms can only be of type degraded");
                            return;
                        }
                    },

                }
            }

            if message.propose_term > self.voted.0 || message.propose_term > self.propose_term {
                //Check the election type

                match message.election_type {

                    ElectionType::Profile => {
                        if self.role == Role::Leader {

                            if self.profile_vote_result(message.replica_profile, 
                                self.monitor.get_profile_y(message.replica_id as usize)
                            ){
                                //Leader has a better profile than the source
                                //Increment the term to invalidate other vote requests
                                self.propose_term += 1;

                                //Send ConfigureReplicaMessage to the source
                                let configure_replica_message = ConfigureReplicaMessage {
                                    leader_address: self.replica_address,
                                    propose_term: self.propose_term,
                                    replica_role: Role::Member, //ToDo - different for each replica
                                };

                                //Serialize the ConfigureReplicaMessage with meta type set
                                let serialized_crm = wrap_and_serialize(
                                    "ConfigureReplicaMessage", 
                                    serde_json::to_string(&configure_replica_message).unwrap()
                                );

                                //Send the ConfigureReplicaMessage to all replicas
                                let _ = self.transport.broadcast(
                                    &self.config,
                                    &mut serialized_crm.as_bytes(),
                                );

                                return;
                            }

                            else {
                                // Leader profile has degraded - no need to lead anymore
                                // Change role to candidate & status to election

                                self.role = Role::Candidate;
                                self.state = ELECTION;

                                //Update the voted tuple
                                self.voted = (message.propose_term, Some(message.replica_id));                

                                //Send the ResponseVoteMessage to the source
                                self.provide_vote_response(message.propose_term, message.replica_address);

                                //Start the leadervotetimer
                                ////let _ = self.leader_vote_timeout.start();

                                //Start the election cycle type start_election_cycle(id, election_type: timeout)

                                return;
                            }

                        }

                        else if self.role == Role::Candidate || self.role == Role::Witness {
                            match message.replica_role {
                                Role::Leader => {
                                    //Ignore the message
                                    println!("Leader can't restart election with any term");
                                    return;
                                },

                                Role::Candidate => {
                                    //Provide the vote result
                                    println!("Processing the vote request - leader profile detected low");
                                    self.process_profile_vote(message); //ToDo - No need to change the role
                                    return;
                                },

                                Role::Witness => {
                                    //Provide the vote result
                                    println!("Processing the vote request - leader profile detected low");
                                    self.process_profile_vote(message); //ToDo - No need to change the role
                                    return;
                                },

                                Role::Member => {
                                    //Ignore the message
                                    println!("Member can't start this election type");
                                    return;
                                },
                            }
                        }

                        else if self.role == Role::Member {
                            //ToDo - for now, ignore the message
                            println!("Member will wait for election results - shouldn't be privileged!");
                            return;
                        }

                    },

                    ElectionType::Offline => {

                        if self.role == Role::Leader {

                            //Increment the term to invalidate other vote requests
                            self.propose_term += 1;

                            //Send ConfigureReplicaMessage to the source
                            let configure_replica_message = ConfigureReplicaMessage {
                                leader_address: self.replica_address,
                                propose_term: self.propose_term,
                                replica_role: Role::Member, //ToDo - different for each replica
                            };

                            //Serialize the ConfigureReplicaMessage with meta type set
                            let serialized_crm = wrap_and_serialize(
                                "ConfigureReplicaMessage", 
                                serde_json::to_string(&configure_replica_message).unwrap()
                            );

                            //Send the ConfigureReplicaMessage to all replicas
                            let _ = self.transport.broadcast(
                                &self.config,
                                &mut serialized_crm.as_bytes(),
                            );

                            return;
                        }

                        else if self.role == Role::Candidate || self.role == Role::Witness{
                            match message.replica_role {
                                Role::Leader => {
                                    //Ignore the message
                                    println!("Leader can't restart election with any term - can only communicate");
                                    return;
                                },

                                Role::Candidate => {
                                    //PollLeaderTimer and/or HeartBeatTimer expired
                                    //Provide the vote result
                                    println!("Processing the vote request - leader detected offline");
                                    self.process_profile_vote(message);
                                    return;
                                },

                                Role::Witness => {
                                    //HeartBeatTimer expired - no candidate elected, witness has started election request
                                    //Provide the vote result
                                    println!("Processing the vote request - leader detected offline");
                                    self.process_profile_vote(message);
                                    return;
                                },

                                Role::Member => {
                                    //Ignore the message
                                    println!("Member can't start this election type");
                                    return;
                                },
                            }
                        }

                        else if self.role == Role::Member {
                            //ToDo - for now, ignore the message
                            println!("Member will wait for election results - shouldn't be privileged!");
                            return;
                        }

                    },

                    ElectionType::Degraded => {
                        println!("Only profile & offline election types accepted");
                        return;
                    },
                    
                    ElectionType::Timeout => {
                        println!("Only profile & offline election types accepted");
                        return;
                    },

                    ElectionType::Normal => {
                        println!("Only profile & offline election types accepted");
                        return;
                    },
                    
                }

            }
        }

    }

    fn handle_response_vote_message (&mut self, message: ResponseVoteMessage) {

        println!("{}: received a ResponseVoteMessage from Replica {}", self.id, message.replica_id);

        if self.replica_address == message.replica_address {
            println!("!{}: received a ResponseVoteMessage from itself", self.id);
            return;
        }

        if self.state != ELECTION {
            println!("!{}: received a ResponseVoteMessage in state {}", self.id, self.state);
            return;
        }

        if self.propose_term != message.propose_term {
            println!("!{}: received a ResponseVoteMessage from {} with stale term", self.id, message.replica_id);
            return;
        }

        if self.role == Role::Leader || self.role == Role::Witness {
            println!("!{}: received a ResponseVoteMessage in leader state", self.replica_address);
            return;
        }

        if let Some(message) = self.leadership_quorum.add_and_check_for_quorum((message.propose_term, message.propose_term), message.replica_id as i32, message.clone()) {
            //We have quorum for leadership - need to communicate to other replicas

            println!("{}: has quorum for leadership", self.id);

            //Change the role to leader
            self.role = Role::Leader;

            //Change the state to normal
            self.state = NORMAL;

            //Create the ConfigureReplicaMessage to be sent to all replicas

            let configure_replica_message = ConfigureReplicaMessage {
                leader_address: self.replica_address,
                propose_term: self.propose_term,
                replica_role: Role::Member, //ToDo - different for each replica
            };

            //Serialize the ConfigureReplicaMessage with meta type set
            let serialized_crm = wrap_and_serialize(
                "ConfigureReplicaMessage", 
                serde_json::to_string(&configure_replica_message).unwrap()
            );

            //Send the ConfigureReplicaMessage to all replicas
            let _ = self.transport.broadcast(
                &self.config,
                &mut serialized_crm.as_bytes(),
            );

            //Start the LeaderLeaseTimer
            self.leader_lease_timeout.start().unwrap();
            
            return;
        }

        else {
            println!("{}: does not have quorum for leadership", self.id);
            return;
        }

    }

    fn start_noler_msg_rcv(&self) {

        let transport = Arc::clone(&self.transport);
        let tx = self.tx.clone();

        thread::spawn(move || {
            let mut buf = [0; 1024];

            loop {
                match transport.receive_from(&mut buf) {
                    Ok((len, _from)) => {
                        let message = String::from_utf8_lossy(&buf[..len]).to_string();
                        tx.send(message).unwrap();
                    },
                    Err(err) => {
                        if err.kind() != std::io::ErrorKind::WouldBlock {
                            println!("Failed to receive data: {:?}", err);
                        }
                    }
                }
            }
        });

        // let mut buf = [0; 1024];

        // loop {
        //     match self.transport.receive_from(&mut buf) {
        //         Ok((size, _)) => {
        //             let message = String::from_utf8_lossy(&buf[..size]).to_string();
        //             self.tx.send(message).unwrap();
        //         },
        //         Err(err) => {
        //             if err.kind() != std::io::ErrorKind::WouldBlock {
        //                 println!("Failed to receive data: {:?}", err);
        //             }
        //         }
        //     }
        // }

        // match self.transport.receive_from(&mut buf) {
        //     Ok((len, _from)) => {
        //         let message = String::from_utf8_lossy(&buf[..len]).to_string();
        //         let wrapper: Result<MessageWrapper, _> = serde_json::from_str(&message);

        //         match wrapper {
        //             Ok(wrapper) => {
        //                 match wrapper.msg_type.as_str() {
        //                     "RequestVoteMessage" => {
        //                         let request_vote_message: Result<RequestVoteMessage, _> = 
        //                             serde_json::from_str(&wrapper.msg_content);

        //                         match request_vote_message {
        //                             Ok(request_vote_message) => {
        //                                 self.handle_request_vote_message(request_vote_message).await;
        //                             },
        //                             Err(err) => {
        //                                 println!("Failed to deserialize request message: {:?}", err);
        //                             }
        //                         }  
        //                     },
        //                     "ResponseVoteMessage" => {
        //                         let response_vote_message: Result<ResponseVoteMessage, _> = 
        //                             serde_json::from_str(&wrapper.msg_content);
                                    
        //                         match response_vote_message {
        //                             Ok(response_vote_message) => {
        //                                 self.handle_response_vote_message(response_vote_message).await;
        //                             },
        //                             Err(err) => {
        //                                 println!("Failed to deserialize the response message: {:?}", err);
        //                             }
        //                         }
        //                     },

        //                     _ => {
        //                         println!("{}: received message of unknown type!", self.id);
        //                     }
        //                 }
        //             },
        //             Err(err) => {
        //                 println!("Failed to deserialize wrapper: {:?}", err);
        //             }
        //         }
        //     },
        //     Err(err) => {
        //         if err.kind() != std::io::ErrorKind::WouldBlock {
        //             println!("Failed to receive data: {:?}", err);
        //         }
        //     }
        // }
    }


    pub fn start_noler_replica (&mut self) {
        println!("Starting a Noler Replica with ID: {} and Address: {}", self.id, self.replica_address);

        //Start the leaderInitTimer
        self.leader_init_timeout.start().unwrap();

        //Start thread for receiving messages
        self.start_noler_msg_rcv();


        loop {
            if let Ok(message) = self.rx.recv() {
                println!("{}: received message: {}", self.id, message);


                match message.as_str() {
                    "leader_init_timeout" => {
                        println!("{}: received leader_init_timeout", self.id);
                        self.start_election_cycle(ElectionType::Normal);
                    },
                    _ => {
                        println!("{}: received message of unknown type!", self.id);
                    }
                }

            }
        }

    }

}