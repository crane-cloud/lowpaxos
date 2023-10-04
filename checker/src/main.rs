use checker::semantics::NolerStore;
use checker::leaderelection_tester::LeaderElectionTester;
use noler::message::ElectionType;
use noler::constants::*;
use noler::role::Role;
use noler::monitor::{Profile, NolerMonitorMatrix};
use noler::config::{ConfigSr, ReplicaSr};
use checker::logsr::{LogSR, LogEntryState};

use checker::kvstoresr::KVStoreSR;

use checker::election_actor::{KvStoreMsg, KvStoreMsg::*, NolerStoreActor};
use stateright::actor::{majority, model_peers, Actor, ActorModel, Id, Network, Out};
use stateright::report::WriteReporter;
use stateright::util::HashableHashMap;
use stateright::{Checker, Expectation, Model};
use std::borrow::Cow;
use std::time::Duration;
use std::net::{SocketAddrV4, Ipv4Addr};
use stateright::actor::spawn;
use rand::Rng;

type Ballot = (u32, u64);
type RequestId = u64;
type Request = (RequestId, Id, Key, Option<Value>);
type Key = u64;
type Value = u64;

use checker::noler_msg_checker::NolerMsg::*;
use checker::noler_msg_checker::ElectionTimer::*;
use checker::noler_msg_checker::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NolerState {
    role: Role,
    status: u8,
    ballot: Ballot,
    voted: (Option<Id>, Option<Ballot>),
    leader: Option<Id>, 
    leadership_quorum: HashableHashMap<Id, Option<Ballot>>,
    propose_quorum: HashableHashMap<(Id, Ballot), Request>,

    monitor: NolerMonitorMatrix,
    config: ConfigSr,

    //Paxos-related
    log:LogSR,
    data: KVStoreSR,

    commit_index: u64,
    execution_index: u64,
}

#[derive(Clone, Debug)]
struct NolerActor {
    peer_ids: Vec<Id>,
}


impl Actor for NolerActor {
    type Msg = KvStoreMsg<RequestId, Key, Value, checker::noler_msg_checker::NolerMsg>;
    type State  = NolerState;
    type Timer = checker::noler_msg_checker::ElectionTimer;

    fn name(&self) -> String {
        "Noler Replica".to_owned()
    }

    fn on_start(&self, _id: Id, o: &mut Out<Self>) -> Self::State {

        let n = self.peer_ids.len();
        //log::info!("{} peers in the network", n);

        let mut matrix = NolerMonitorMatrix::new();

        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }

                //let profile = Profile::new(rand::thread_rng().gen_range(0..100));
                let profile = Profile::new(((j.wrapping_mul(37)^i.wrapping_mul(73)) % 101) as u8);

                matrix.set(self.peer_ids[i], profile).unwrap();
            }
        }

        let mut replicas = Vec::new();

        for i in 0..n {
            replicas.push(ReplicaSr {
                id: self.peer_ids[i],
                status: INITIALIZATION,
                role: Role::new(),
                profile: {
                    if let Some(profile) = matrix.get(&self.peer_ids[i]) {
                        profile.get_x()
                    }
                    else { 0 }
                }
            });
        }

        let init_timer = rand::thread_rng().gen_range(1..10);

        o.set_timer(LeaderInitTimeout,
            Duration::from_secs(init_timer)..Duration::from_secs(init_timer));

        NolerState {
            role: Role::Member,
            status: INITIALIZATION,
            ballot: (0, 0),
            voted: (None, None),
            leader: None,
            leadership_quorum: Default::default(),
            propose_quorum: Default::default(),

            monitor: matrix,

            config: ConfigSr::new((0, 0), n, replicas),

            log: LogSR::new(),
            data: KVStoreSR::new(),
            commit_index: 0,
            execution_index: 0,
        }
    }

    fn on_msg(
        &self,
        idx: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        //timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {

        match msg {
            KvStoreMsg::Set(request_id, key, value) if state.status == NORMAL && state.leader.is_some() => {
                log::info!("{}: received Put request from {}", idx, src);
                let state = state.to_mut();

                if state.role != Role::Leader {
                    //Forward to the leader
                    o.send(
                        state.leader.unwrap(),
                        Internal(SetInternal {
                            src,
                            request_id,
                            key,
                            value,
                        }),
                    );
                    return;
                }


                //let request_entry = state.log.find_by_request_id(request_id);
                //let request_entry = state.log.find_by_request((request_id, src, Option::from(value)));
                let request_entry = state.log.find_by_request((request_id, src, key, Option::from(value)));

                if request_entry.is_some() {
                    log::info!("{}: SET request already in the log with ballot: {:?}", idx, request_entry.unwrap().ballot);

                    let entry_state = &request_entry.unwrap().state;

                    //Check the status of the entry
                    match entry_state {
                        LogEntryState::Propose => {
                            // Resend the request to peers
                            o.broadcast(
                                &self.peer_ids,
                                &Internal(Propose {
                                    id: idx,
                                    ballot: request_entry.unwrap().ballot,
                                    request: request_entry.unwrap().request,
                                }),
                            );
                        },

                        LogEntryState::Committed => {
                            // A reply response is already logged - send it to the client
                            let reply = request_entry.unwrap().reply.clone().unwrap();

                            //Send the response to the client from log
                            o.send(
                                src,
                                reply,
                            );
                        },

                        LogEntryState::Executed => {
                            // A reply response is already logged - send it to the client
                            let reply = request_entry.unwrap().reply.clone().unwrap();

                            //Send the response to the client from log
                            o.send(
                                src,
                                reply,
                            );
                        }

                        _ => {
                            log::info!("{}: PUT request still under processing", idx);
                            //return;
                        }
                    }
                }

                else {
                    //Add to the log and broadcast the request to peers
                    log::info!("{}: adding PUT request to the log", idx);
                    let ballot = (state.ballot.0, state.ballot.1 + 1);
                    let request = (request_id, src, key, Option::from(value));

                    state.log.append(ballot, request, None, LogEntryState::Propose);

                    o.broadcast(
                        &self.peer_ids,
                        &Internal(Propose {
                            id: idx,
                            ballot,
                            request,
                        }),
                    )
                }
            }

            Internal(SetInternal {request_id, src, key, value}) if state.status == NORMAL && state.role == Role::Leader =>  {
                let state = state.to_mut();

                let request_entry = state.log.find_by_request((request_id, src, key, Option::from(value)));

                if request_entry.is_some() {
                    log::info!("{}: PUT request already in the log with ballot: {:?}", idx, request_entry.unwrap().ballot);

                    let entry_state = &request_entry.unwrap().state;

                    //Check the status of the entry
                    match entry_state {
                        LogEntryState::Propose => {
                            // Resend the request to peers
                            o.broadcast(
                                &self.peer_ids,
                                &Internal(Propose {
                                    id: idx,
                                    ballot: request_entry.unwrap().ballot,
                                    request: request_entry.unwrap().request,
                                }),
                            );
                        },

                        LogEntryState::Committed => {
                            // A reply response is already logged - send it to the client
                            let reply = request_entry.unwrap().reply.clone().unwrap();

                            //Send the response to the client from log
                            o.send(
                                src,
                                reply,
                            );
                        },

                        LogEntryState::Executed => {
                            // A reply response is already logged - send it to the client
                            let reply = request_entry.unwrap().reply.clone().unwrap();

                            //Send the response to the client from log
                            o.send(
                                src,
                                reply,
                            );
                        }

                        _ => {
                            log::info!("{}: PUT request still under processing", idx);
                            //return;
                        }
                    }
                }

                else {
                    //Add to the log and broadcast the request to peers
                    log::info!("{}: adding request to the log", idx);
                    let ballot = (state.ballot.0, state.ballot.1 + 1);
                    let request = (request_id, src, key, Option::from(value));
                    state.log.append(ballot, request, None, LogEntryState::Propose);

                    o.broadcast(
                        &self.peer_ids,
                        &Internal(Propose {
                            id: idx,
                            ballot,
                            request,
                        }),
                    )
                }
            }

            KvStoreMsg::Get(request_id, key) if state.status == NORMAL && state.leader.is_some() => {
                log::info!("{}: received Get request from {} with Get:[{}]", idx, src, key);
                let state = state.to_mut();

                if state.role != Role::Leader {
                    //Forward to the leader
                    o.send(
                        state.leader.unwrap(),
                        Internal(GetInternal {
                            id: idx,
                            src,
                            request_id,
                            key,
                        }),
                    );
                    return;
                }

                //We need to check the status of the put request for the request_id in the message
                let request_entry = state.log.get_by_request_id(request_id, key);

                if request_entry.is_some(){
                    //We retrieve the value from the kvstore | there is a PutOk

                    //Send the response to the client
                    o.send(
                        src,
                        state.data.get(request_id, key),
                    );
                }

                else {
                    log::info!("{}: GET request not in the log", idx);
                }

                // let request_entry = state.log.find_by_request((request_id, src, None));

                // if request_entry.is_some() {
                //     log::info!("{}: GET request already in the log with ballot: {:?}", idx, request_entry.unwrap().ballot);

                //     let entry_state = &request_entry.unwrap().state;

                //     //Check the status of the entry
                //     match entry_state {
                //         LogEntryState::Propose => {
                //             // Resend the request to peers
                //             o.broadcast(
                //                 &self.peer_ids,
                //                 &Internal(Propose {
                //                     id: idx,
                //                     ballot: request_entry.unwrap().ballot,
                //                     request: request_entry.unwrap().request,
                //                 }),
                //             );
                //         },

                //         LogEntryState::Committed => {
                //             // A reply response is already logged - send it to the client
                //             let reply = request_entry.unwrap().reply.clone().unwrap();

                //             //Send the response to the client from log
                //             o.send(
                //                 src,
                //                 reply,
                //             );
                //         },

                //         LogEntryState::Executed => {
                //             // A reply response is already logged - send it to the client
                //             let reply = request_entry.unwrap().reply.clone().unwrap();

                //             //Send the response to the client from log
                //             o.send(
                //                 src,
                //                 reply,
                //             );
                //         }

                //         _ => {
                //             log::info!("{}: GET request still under processing", idx);
                //             //return;
                //         }
                //     }
                // }

                // else {
                //     //Add to the log and broadcast the request to peers
                //     log::info!("{}: adding GET request to the log", idx);
                //     let ballot = (state.ballot.0, state.ballot.1 + 1);
                //     let request = (request_id, src, None);

                //     state.log.append(ballot, request, None, LogEntryState::Propose);

                //     o.broadcast(
                //         &self.peer_ids,
                //         &Internal(Propose {
                //             id: idx,
                //             ballot,
                //             request,
                //         }),
                //     )
                // }
            }

            Internal(GetInternal {id, src, request_id, key}) => if state.status == NORMAL && state.role == Role::Leader {
                let state = state.to_mut();

                log::info!("{}: received Get Internal request from {} and origin {} with GET:[{}]", idx, id, src, key);

                //We need to check the status of the put request for the request_id in the message
                let request_entry = state.log.get_by_request_id(request_id, key);

                if request_entry.is_some(){
                    //We retrieve the value from the kvstore | there is a PutOk
                    log::info!("{}: Sending GET response to client {}", idx, src);
                    //Send the response to the client
                    o.send(
                        src,
                        state.data.get(request_id, key),
                    );

                    log::info!("{}: Sent GET response to client {}", idx, src);
                }

                else {
                    log::info!("{}: GET request not in the log", idx);
                }

                // let request_entry = state.log.find_by_request((request_id, src, None));

                // if request_entry.is_some() {
                //     log::info!("{}: GET request already in the log with ballot: {:?}", idx, request_entry.unwrap().ballot);

                //     let entry_state = &request_entry.unwrap().state;

                //     //Check the status of the entry
                //     match entry_state {
                //         LogEntryState::Propose => {
                //             // Resend the request to peers
                //             o.broadcast(
                //                 &self.peer_ids,
                //                 &Internal(Propose {
                //                     id: idx,
                //                     ballot: request_entry.unwrap().ballot,
                //                     request: request_entry.unwrap().request,
                //                 }),
                //             );
                //         },

                //         LogEntryState::Committed => {
                //             // A reply response is already logged - send it to the client
                //             let reply = request_entry.unwrap().reply.clone().unwrap();

                //             //Send the response to the client from log
                //             o.send(
                //                 src,
                //                 reply,
                //             );
                //         },

                //         LogEntryState::Executed => {
                //             // A reply response is already logged - send it to the client
                //             let reply = request_entry.unwrap().reply.clone().unwrap();

                //             //Send the response to the client from log
                //             o.send(
                //                 src,
                //                 reply,
                //             );
                //         }

                //         _ => {
                //             log::info!("{}: PUT request still under processing", idx);
                //             //return;
                //         }
                //     }
                // }

                // else {
                //     //Add to the log and broadcast the request to peers
                //     log::info!("{}: adding GET request to the log", idx);
                //     let ballot = (state.ballot.0, state.ballot.1 + 1);
                //     let request = (request_id, src, None);

                //     state.log.append(ballot, request, None, LogEntryState::Propose);

                //     o.broadcast(
                //         &self.peer_ids,
                //         &Internal(Propose {
                //             id: idx,
                //             ballot,
                //             request,
                //         }),
                //     )
                // }
            }

            Internal(RequestVote (RequestVoteMessage {id, replica_role, ballot, election_type, profile })) => {

                let state = state.to_mut();

                if state.voted.1.is_none() {

                    o.cancel_timer(LeaderInitTimeout);

                    if state.ballot < ballot {

                        state.status = ELECTION;

                        //state.voted = (Some(id), Some(ballot));

                        if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                            if profile >= state.monitor.get(&id).unwrap().get_x() {
                                state.voted = (Some(id), Some(ballot));
                                o.send(
                                    src,
                                    Internal(ResponseVote (ResponseVoteMessage {
                                        id: idx,
                                        ballot,
                                    })),
                                );
                            }

                            else {
                                state.role = Role::Candidate;

                                o.set_timer(LeaderVoteTimeout, 
                                    Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                            }
                        }

                        else {
                            state.voted = (Some(id), Some(ballot));

                            o.send(
                                src,
                                Internal(ResponseVote (ResponseVoteMessage {
                                    id: idx,
                                    ballot,
                                })),
                            );

                            o.set_timer(LeadershipVoteTimeout, 
                                Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));
                        }


                    }

                    else { 
                        log::info!("{} has a higher ballot", idx);
                        return; 
                    }
                }

                else if state.voted.1.is_some() {
                    if ballot == state.voted.1.unwrap() && id == state.voted.0.unwrap() {
                        //duplicate vote request - resend response
                        o.send(
                            src,
                            Internal(ResponseVote (ResponseVoteMessage {
                                id: idx,
                                ballot,
                            })),
                        );
                    }

                    if ballot == state.voted.1.unwrap() {
                        match replica_role  {
                            Role::Leader => {
                                log::info!("{} leader can't restart an election", idx);
                                return;
                            },

                            Role::Candidate => {
                                if election_type == ElectionType::Timeout {

                                    //process the vote

                                    if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                                        if profile >= state.monitor.get(&id).unwrap().get_x() {
                                            state.voted = (Some(id), Some(ballot));
                                            o.send(
                                                src,
                                                Internal(ResponseVote (ResponseVoteMessage {
                                                    id: idx,
                                                    ballot,
                                                })),
                                            );
                                        }

                                        else {
                                            state.role = Role::Candidate;

                                            o.set_timer(LeaderVoteTimeout, 
                                                Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                        }
                                    }
                                }

                                else { 
                                    log::info!("{} only timeout election allowed", idx);
                                    return; 
                                }
                            },

                            Role::Witness => {
                                log::info!("{} witness can't start this election", idx);
                                return;
                            },
                            
                            Role::Member => {
                                if election_type == ElectionType::Degraded {

                                    state.voted = (Some(id), Some(ballot));

                                    o.send(
                                        src,
                                        Internal(ResponseVote (ResponseVoteMessage {
                                            id: idx,
                                            ballot,
                                        })),
                                    );
                                }
                                    
                                else { 
                                    log::info!("{} only degraded election allowed here", idx);
                                    return; 
                                }
                            }
                        }
                    }

                    else if ballot > state.voted.1.unwrap() {
                        match election_type {
                            ElectionType::Profile => {
                                if state.role == Role::Leader {

                                    if profile > state.monitor.get(&id).unwrap().get_x() {
                                        log::info!("{}: new member {} with better profile seeks election", idx, id);

                                        state.status = ELECTION;

                                        state.role = Role::Candidate;

                                        state.voted = (Some(id), Some(ballot));

                                        o.send(
                                            src,
                                            Internal(ResponseVote (ResponseVoteMessage {
                                                id: idx,
                                                ballot,
                                            })),
                                        );

                                        o.set_timer(LeaderVoteTimeout, 
                                            Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                    }

                                    else {
                                        log::info!("{}: new member {} with worse profile seeks election", idx, id);
                                        //affirm leadership
                                        state.ballot = (state.ballot.0 + 1, state.ballot.1 + 1);

                                        //update the config
                                        let mut config = state.config.clone();

                                        let role_mapping = vec![
                                            (0, 0, Role::Leader),
                                            (1, ((config.n - 1)/2), Role::Candidate),
                                            ((config.n - 1)/2 + 1, (config.n - 1), Role::Witness),
                                        ];

                                        for replica in config.replicas.iter_mut() {
                                            if id == idx {
                                                replica.profile = 100;
                                            }

                                            else {
                                                replica.profile = state.monitor.get(&id).unwrap().get_x();
                                            }
                                        }

                                        //Sort the config by profile
                                        config.replicas.sort_by(|a, b| b.profile.partial_cmp(&a.profile).unwrap());

                                        //Sort the roles
                                        for (x, y, z) in role_mapping {
                                            for i in x..=y {
                                                config.replicas[i as usize].role = z;
                                            }
                                        }

                                        config.ballot = state.ballot;

                                        o.broadcast(
                                            &self.peer_ids,
                                            &Internal(Config(ConfigMessage {
                                                leader: idx,
                                                config,
                                            }))
                                        );

                                    }
                                }

                                else if state.role == Role::Candidate || state.role == Role::Witness {
                                    match replica_role {
                                        Role::Leader => {
                                            log::info!("{}: leader {} can not seek election", idx, id);
                                            return;
                                        },

                                        Role::Member => {
                                            log::info!("{}: member {} can not seek election", idx, id);
                                            return;
                                        },
                                        // Witness or Candidate
                                        _=> {
                                            log::info!("{}: witness or candidate processing election request", idx);
                                            //process the vote

                                            if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                                                if profile >= state.monitor.get(&id).unwrap().get_x() {
                                                    state.voted = (Some(id), Some(ballot));
                                                    o.send(
                                                        src,
                                                        Internal(ResponseVote (ResponseVoteMessage {
                                                            id,
                                                            ballot,
                                                        })),
                                                    );
                                                }

                                                else {
                                                    state.role = Role::Candidate;

                                                    o.set_timer(LeaderVoteTimeout, 
                                                        Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                                }
                                            }

                                            else {
                                                //Provide the response immediately

                                                state.voted = (Some(id), Some(ballot));

                                                o.send(
                                                    src,
                                                    Internal(ResponseVote (ResponseVoteMessage {
                                                        id,
                                                        ballot,
                                                    })),
                                                );
                                            }
                                        }
                                    }
                                
                                }

                                else if state.role == Role::Member {
                                    log::info!("{}: member can't service election request", idx);
                                    return; //ToDo: maybe just vote
                                }
                            }

                            ElectionType::Offline => {
                                if state.role == Role::Leader {
                                    log::info!("{}: leader is alive", idx);

                                    //affirm leadership
                                    state.ballot = (state.ballot.0 + 1, state.ballot.1 + 1);

                                    //update the config
                                    let mut config = state.config.clone();

                                    let role_mapping = vec![
                                        //(0, 0, Role::Leader),
                                        (1, ((config.n - 1)/2), Role::Candidate),
                                        ((config.n - 1)/2 + 1, (config.n - 1), Role::Witness),
                                    ];

                                    for replica in config.replicas.iter_mut() {
                                        if id == idx {
                                            replica.profile = 100; //Not in the config.replicas as it is the same host
                                        }

                                        else {
                                            replica.profile = state.monitor.get(&id).unwrap().get_x();
                                        }
                                    }

                                    //Sort the config by profile
                                    config.replicas.sort_by(|a, b| b.profile.partial_cmp(&a.profile).unwrap());

                                    //Sort the roles
                                    for (x, y, z) in role_mapping {
                                        for i in x..=y {
                                            config.replicas[i as usize].role = z;
                                        }
                                    }

                                    config.ballot = state.ballot;

                                    o.broadcast(
                                        &self.peer_ids,
                                        &Internal(Config(ConfigMessage {
                                            leader: idx,
                                            config,
                                        }))
                                    );
                                }

                                else if state.role == Role::Candidate || state.role == Role::Witness {
                                    match replica_role {
                                        Role::Leader => {
                                            log::info!("{}: leader {} can not seek offline election", idx, id);
                                            return;
                                        },

                                        Role::Member => {
                                            log::info!("{}: member {} can not seek offline election", idx, id);
                                            return;
                                        },

                                        //Candidate or Witness
                                        _ => {
                                            log::info!("{}: witness or candidate processing offline election request", idx);
                                            //process the vote

                                            if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                                                if profile >= state.monitor.get(&id).unwrap().get_x() {
                                                    state.voted = (Some(id), Some(ballot));
                                                    o.send(
                                                        src,
                                                        Internal(ResponseVote (ResponseVoteMessage {
                                                            id,
                                                            ballot,
                                                        })),
                                                    );
                                                }

                                                else {
                                                    state.role = Role::Candidate;

                                                    o.set_timer(LeaderVoteTimeout, 
                                                        Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                                }
                                            }

                                            else {
                                                //Provide the response immediately

                                                state.voted = (Some(id), Some(ballot));

                                                o.send(
                                                    src,
                                                    Internal(ResponseVote (ResponseVoteMessage {
                                                        id,
                                                        ballot,
                                                    })),
                                                );
                                            }
                                        }
                                    }
                                }

                                else if state.role == Role::Member {
                                    log::info!("{}: member can't service offline election request", idx);
                                    return; //ToDo: maybe just vote
                                }
                            }

                            ElectionType::Timeout => {
                                log::info!("{}: timeout election request not allowed", idx);
                                return;
                            }

                            ElectionType::Degraded => {
                                log::info!("{}: degraded election request not allowed", idx);
                                return;
                            }

                            ElectionType::Normal => {
                                log::info!("{}: normal election request not allowed", idx);
                                return;
                            }
                        }
                    }
                }
            }

            Internal(ResponseVote (ResponseVoteMessage { id, ballot })) if ballot == (state.ballot.0 + 1, state.ballot.1 + 1) => {
                let state = state.to_mut();
                
                log::info!("{} now checking for quorum....", idx);

                state.leadership_quorum.insert(id, Some(ballot));

                //log::info!("{} has {:?} votes", idx, state.leadership_quorum.len());
                    
                if state.leadership_quorum.len() == majority(self.peer_ids.len() + 1) {
                    state.status = NORMAL;
                    state.role = Role::Leader;
                    state.leader = Some(idx);
                    state.ballot = ballot;

                    let mut config = state.config.clone();

                    let role_mapping = vec![
                        //(0, 0, Role::Leader),
                        (0, ((config.n/2) - 1) , Role::Candidate),
                        ((config.n)/2, (config.n - 1), Role::Witness),
                    ];

                    //Sort the config by profile
                    config.replicas.sort_by(|a, b| b.profile.partial_cmp(&a.profile).unwrap());

                    //Sort the roles
                    for (x, y, z) in role_mapping {
                        for i in x..=y {
                            config.replicas[i as usize].role = z;
                        }
                    }

                    //Update the config ballot
                    config.ballot = state.ballot;

                    o.broadcast(&self.peer_ids, 
                        &Internal(Config(ConfigMessage {
                            leader: idx,
                            config: config,
                        }))
                    );

                }

                else {
                    log::info!("{} is still waiting for quorum....", idx);
                }

            }

            Internal(Config(ConfigMessage { leader, config })) if state.role != Role::Leader => {
                let state = state.to_mut();

                o.cancel_timer(LeadershipVoteTimeout);
                o.cancel_timer(LeaderVoteTimeout);
                o.cancel_timer(LeaderInitTimeout);

                if config.ballot > state.ballot {
                    state.config = config.clone();
                    state.leader = Some(leader);
                    state.ballot = config.ballot;

                    for replica in state.config.replicas.iter() {
                        if replica.id == idx {
                            state.role = replica.role;
                        }
                    }

                    match state.role {
                        Role::Leader => {
                            log::info!("{}: now a leader - ignore", idx);
                            return; //should be unreachable
                        },

                        Role::Candidate => {
                            //change status to normal
                            state.status = NORMAL;

                            log::info!("{}: now a candidate - start timers", idx);
                            // o.set_timer(POLL_LEADER_TIMER
                            //     , Duration::from_secs(POLL_LEADER_TIMEOUT)..Duration::from_secs(POLL_LEADER_TIMEOUT));
                            // o.set_timer(POLL_LEADER_TIMEOUT, 
                            //     Duration::from_secs(POLL_LEADER_TIMEOUT)..Duration::from_secs(POLL_LEADER_TIMEOUT));
                            // o.set_timer(HEARTBEAT_TIMEOUT, 
                            //     Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));    
                        }

                        Role::Witness => {
                            //change status to normal
                            state.status = NORMAL;
                            log::info!("{}: now a witness - start timers", idx);
                            //o.set_timer(HEARTBEAT_TIMEOUT, 
                            //    Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));
                        }

                        Role::Member => {
                            log::info!("{}: config can't have a member role", idx);
                            return;
                        }
                    }
                }

                else {
                    log::info!("{}: config ballot is lower than current ballot", idx);
                    return;
                }
            }

            Internal(Propose {id, ballot, request}) if state.status == NORMAL && state.role != Role::Leader => {
                let state = state.to_mut();

                if ballot == state.ballot {
                    //Request has already been processed & committed
                    log::info!("{}: propose request already committed|agreed", idx);
                }

                else if ballot.0 == state.ballot.0 && ballot.1 > state.ballot.1 { // how far behind can a member be?
                    //Check if the entry is already in the log
                    let entry = state.log.find(ballot);

                    if entry.is_some() {
                        log::info!("{}: entry already in the log", idx); //ToDo: check the status of the entry

                        let entry_state = &entry.unwrap().state;

                        match entry_state {
                            LogEntryState::Proposed => {
                                //Resend the ProposeOk
                                o.send(
                                    id,
                                    Internal(ProposeOk {
                                        id: idx,
                                        ballot,
                                        commit_index: state.commit_index,
                                        request: entry.unwrap().request,
                                    }),
                                );
                            },
                            _ => {}
                        }
                    }

                    else {

                        //Add to the log and respond with ProposeOk
                        state.log.append(ballot, request, None, LogEntryState::Proposed);

                        log::info!("{}: entry added to the log - send proposeok to leader", idx);
                        o.send(
                            id,
                            Internal(ProposeOk {
                                id: idx,
                                ballot,
                                commit_index: state.commit_index,
                                request,
                            }),
                        );
                    }

                }

                else if ballot > state.ballot {
                    //Request has already been processed & committed
                    log::info!("{}: a new leader - request for state", idx);
                }

                else {
                    log::info!("{}: stale propose request", idx);
                    //return;
                }


            }

            Internal (ProposeOk {id, ballot, commit_index, request}) if state.status == NORMAL && state.role == Role::Leader => {
                let state = state.to_mut();

                if ballot.0 == state.ballot.0 && ballot.1 == state.ballot.1 + 1 {
                    log::info!("{}: leader received ProposeOk from {}", idx, id);
                    //Check if the entry is already in the log

                    let entry = state.log.find(ballot); //Check this request & ballot match

                    if entry.is_some() && entry.unwrap().state == LogEntryState::Propose {
                        //Check the quorum on the ProposeOk

                        state.propose_quorum.insert((id, ballot), request);
                        log::info!("{}: propose quorum has {} votes", idx, state.propose_quorum.len());

                        //print the propose quorum
                        for (k, v) in state.propose_quorum.iter() {
                            log::info!("{}: {:?} -> {:?}", idx, k, v);
                        }

                        if state.propose_quorum.len() == majority(self.peer_ids.len() + 1) {

                            log::info!("{}: consensus reached on log: {:?} <> request: {:?}", idx, entry.unwrap().request, request);

                            //Update the log with committed
                            state.log.set_status(ballot, LogEntryState::Committed);

                            //Update current ballot & commit index
                            state.ballot = ballot;
                            state.commit_index = ballot.1;

                            //Determine the type of operation
                            if request.3.is_none() {
                                //GET request - execute on the kvstore
                                let result = state.data.get(request.0, request.2);

                                //Set the log with the result of the kvstore execution
                                state.log.set_reply(ballot, result.clone());

                                //Request has now been executed
                                state.log.set_status(ballot, LogEntryState::Executed);
                                state.execution_index = ballot.1;

                                //Send response to the client
                                o.send(
                                    request.1,
                                    result,
                                );

                                //Broadcast commit to the peers
                                o.broadcast(
                                    &self.peer_ids,
                                    &Internal(Commit {
                                        id: idx,
                                        ballot,
                                        request,
                                    })
                                );
                            }

                            else {
                                //PUT request - execute on the kvstore
                                let result = state.data.set(request.0, request.2, request.3.unwrap());

                                //Set the log with the result of the kvstore execution
                                state.log.set_reply(ballot, result.clone());

                                //Request has now been executed
                                state.log.set_status(ballot, LogEntryState::Executed);

                                //Provide a response to the client -
                                log::info!("{}: sending response to client: {}", idx, request.1);

                                o.send(
                                    request.1,
                                    result,
                                );

                                //Broadcast commit to the peers
                                o.broadcast(
                                    &self.peer_ids,
                                    &Internal(Commit {
                                        id: idx,
                                        ballot,
                                        request,
                                    })
                                );
                            }

                            //Clear the propose quorum
                            state.propose_quorum.clear();
                        }
                    }

                    else {
                        log::info!("{}: entry not in the log", idx);
                        //return;
                    }
                }

                else {
                    log::info!("{}: stale request", idx);
                    //return;
                }

            }

            Internal (Commit {id, ballot, request}) if state.status == NORMAL && state.role != Role::Leader => {
                let state = state.to_mut();

                if ballot.0 == state.ballot.0 && ballot.1 > state.ballot.1 {
                    log::info!("{}: received Commit from {}", idx, id);
                    //Find the entry in the log

                    let entry = state.log.find(ballot);

                    if entry.is_some() {
                        //Check the status of the entry 

                        if entry.unwrap().state == LogEntryState::Proposed {
                            //Update the log entry to committed

                            state.log.set_status(ballot, LogEntryState::Committed);

                            //Update current ballot & commit index
                            state.ballot = ballot;
                            state.commit_index = ballot.1;

                            match state.role {
                                Role::Candidate => {
                                    //Check the type of request
                                    if request.3.is_none() {
                                        //GET request - execute on the kvstore
                                        let result = state.data.get(request.0, request.2);

                                        //Set the log with the result of the kvstore execution
                                        state.log.set_reply(ballot, result.clone());

                                        //Request has now been executed
                                        state.log.set_status(ballot, LogEntryState::Executed);
                                        state.execution_index = ballot.1;
                                    }

                                    else {
                                        //PUT request - execute on the kvstore
                                        let result = state.data.set(request.0, request.2, request.3.unwrap());

                                        //Set the log with the result of the kvstore execution
                                        state.log.set_reply(ballot, result.clone());

                                        //Request has now been executed
                                        state.log.set_status(ballot, LogEntryState::Executed);
                                        state.execution_index = ballot.1;
                                    }
                                }

                                Role::Witness => {
                                    //No need to update the kvstore app
                                    //return;
                                    log::info!("{}: witness doesn't need to execute {}", idx, id);
                                }
                                Role::Member => {
                                    log::info!("{}: member... requesting for configuration information {}", idx, id);
                                    //No need to update the kvstore app
                                    //ToDo: Request state from the leader
                                    //return;
                                }

                                Role::Leader => {
                                    //log::info!("{}: leader can't receive Commit from {}", idx, id);
                                    assert_eq!(state.role, Role::Leader, "leader can't receive Commit from {}", id);
                                }   
                            }
                        }

                        else {
                            log::info!("{}: entry not proposed", idx);
                            return;
                        }
                    }

                    else {
                        log::info!("{}: entry not in the log", idx);
                        return;
                    }
                }

                else {
                    log::info!("{}: stale request", idx);
                    return;
                }
            }

            // Internal (CommitOk { id, ballot, request }) if state.status == NORMAL => {
            //     let state = state.to_mut();

            //     if state.role != Role::Leader {
            //         log::info!("{}: only leader can receive CommitOk", idx);
            //         return;
            //     }

            //     if id == idx {
            //         log::info!("{}: leader can't receive CommitOk from itself", idx);
            //         return;
            //     }

            //     if ballot == state.ballot {
                    
            //         //Check if the entry is already in the log
            //         let entry = state.log.find(ballot);

            //         if entry.is_some() {
            //             //Check the state of the entry

            //             if entry.unwrap().state == LogEntryState::Committed {
            //                 //Check the quorum on the CommitOk

            //                 state.commit_quorum.insert(id, request);

            //                 if state.commit_quorum.len() == majority(self.peer_ids.len()) {

            //                     //Execute on the kvstore
            //                     state.data.set(request.0.to_string(), request.2.to_string());

            //                     //Update the log with executed
            //                     state.log.set_status(ballot, LogEntryState::Executed);

            //                     //update the execution index
            //                     state.execution_index = ballot.1;
            //                 }

            //                 else {
            //                     log::info!("{}: commit quorum not reached", idx);
            //                     return;
            //                 }
            //             }

            //             else {
            //                 log::info!("{}: entry not earlier committed", idx);
            //                 return;
            //             }
            //         }

            //     }

            //     else {
            //         log::info!("{}: stale ballot request", idx);
            //         return;
            //     }
            // }

            _ => {} //Other msg use cases
        }
    }

    fn on_timeout(
            &self,
            id: Id,
            state: &mut Cow<Self::State>,
            timer: &Self::Timer,
            o: &mut Out<Self>,
        ) {
            match timer {

               LeaderInitTimeout => {
                    //o.set_timer(LeaderInitTimeout, model_timeout());
                    log::info!("{} Now starting a new normal election", id);

                    for &dst in &self.peer_ids {
                        if id == dst {
                            continue;
                        }
                        o.send(dst,
                            Internal(RequestVote (RequestVoteMessage {
                                id: id,
                                replica_role: Role::Member,
                                ballot: (state.ballot.0 + 1, state.ballot.1 + 1),
                                election_type: ElectionType::Normal,
                                profile: {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }
                            }))
                        );
                    }
                }
                LeadershipVoteTimeout => {
                    //o.set_timer(LeadershipVoteTimeout, model_timeout());

                    for &dst in &self.peer_ids {
                        o.send(dst,
                            Internal(RequestVote (RequestVoteMessage {
                                id: id,
                                replica_role: Role::Member,
                                ballot: (state.ballot.0 + 1, state.ballot.1 + 1),
                                election_type: ElectionType::Timeout,
                                profile: {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }
                            }))
                        );
                    }

                }

                LeaderVoteTimeout => {
                    //o.set_timer(LeaderVoteTimeout, model_timeout());

                    for &dst in &self.peer_ids {
                        o.send(dst,
                            Internal(RequestVote (RequestVoteMessage {
                                id: id,
                                replica_role: Role::Candidate,
                                ballot: (state.ballot.0 + 1, state.ballot.1 + 1),
                                election_type: ElectionType::Timeout,
                                profile: {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }
                            }))
                        );
                    }
                }

                _ => {}//ToDo
            }
    }


}

#[derive(Clone, Debug)]
struct NolerModelCfg {
    client_count: usize,
    peer_count: usize,
    network: Network<<NolerActor as Actor>::Msg>,
}

//ActorModel<RegisterActor<NolerActor>, Self, LinearizabilityTester<Id, Register<ValueNoler>>>
impl NolerModelCfg {
    fn into_model(
        self,
    ) -> ActorModel<
        NolerStoreActor<NolerActor>,
        Self,
        LeaderElectionTester<Id, NolerStore<Key, Value>>>
    {
        ActorModel::new(
            self.clone(),
            LeaderElectionTester::new(NolerStore(0, 0)),
        )
        .actors((0..self.peer_count).map(|i| {
            NolerStoreActor::Server(NolerActor {
                peer_ids: model_peers(i, self.peer_count),
            })
        }))
        .actors((0..self.client_count).map(|_| NolerStoreActor::Client {
            set_count: 1,
            server_count: self.peer_count,
        }))
        .init_network(self.network)
        // .property(Expectation::Always, "linearizable", |_, state| {
        //     state.history.serialized_history().is_some()
        // })
        // .property(Expectation::Always, "leader elected", |_, state| {
        //     state.network.iter_deliverable().any(|env| {
        //         if let NolerMsg::Config(ConfigMessage { leader, .. }) = env.msg {
        //             true
        //         } else {
        //             false
        //         }
        //     })
        // })
        // .property(Expectation::Sometimes, "leader elected", |_, state| {
        //     for env in state.network.iter_deliverable() {
        //         if let RegisterMsg::Internal(NolerMsg::Config(ConfigMessage {leader, ..})) = env.msg {
        //             log::info!("Leader elected: {}", leader);
        //             return true;
        //         }
        //     }
        //     false
        // })
        .property(Expectation::Sometimes, "value chosen", |_, state| {
            for env in state.network.iter_deliverable() {
                if let KvStoreMsg::GetOk(_req_id, key, value) = env.msg {
                    if *value != 0 && *key != 0 { 
                        return true;
                    }
                }
            }
            false
        })
        .record_msg_in(KvStoreMsg::record_returns)
        .record_msg_out(KvStoreMsg::record_invocations)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn can_model_noler() {
        let checker = NolerModelCfg {
            client_count: 1,
            peer_count: 5,
            network: Network::new_unordered_nonduplicating([]),
        }
        .into_model()
        .checker()
        .spawn_dfs()
        .join();

        checker.assert_properties();
    }
}


fn main() -> Result<(), pico_args::Error> {

    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let mut args = pico_args::Arguments::from_env();

    match args.subcommand()?.as_deref() {
        Some("check") => {
            let network = args
                .opt_free_from_str()?
                .unwrap_or_else(|| Network::new_unordered_nonduplicating([]));
            println!("Model checking Noler with network {:?}", network);

            NolerModelCfg {
                client_count: 1,
                peer_count: 5,
                network,
            }
            .into_model()
            .checker()
            .threads(num_cpus::get())
            .spawn_dfs()
            .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }

        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            let network = args
                .opt_free_from_str()?
                .unwrap_or_else(|| Network::new_unordered_nonduplicating([]));
            println!("Exploring Noler state space with network {:?} and address {}", network, address);

            NolerModelCfg {
                client_count: 1,
                peer_count: 5,
                network,
            }
            .into_model()
            .checker()
            .threads(num_cpus::get())
            .serve(address);
        }

        Some("spawn") => {
            let port = 3000;

            println!("A set of replicas that implement Noler are listening on port {}", port);
            println!("You can send them messages with `nc localhost {}`", port);
            println!(
                "{}",
                serde_json::to_string(&KvStoreMsg::Set::<RequestId, Key, Value, ()>(1, 1, 111)).unwrap()
            );
            println!(
                "{}",
                serde_json::to_string(&KvStoreMsg::Get::<RequestId, Key, Value, ()>(1, 1)).unwrap()
            );
            println!();

            let id0 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
            let id1 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 1));
            let id2 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 2));
            let id3 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 3));
            let id4 = Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port + 4));

            spawn(
                serde_json::to_vec,
                |bytes| serde_json::from_slice(bytes),
                vec![
                    (
                        id0,
                        NolerActor {
                            peer_ids: vec![id1, id2, id3, id4],
                        },
                    ),
                    (
                        id1,
                        NolerActor {
                            peer_ids: vec![id0, id2, id3, id4],
                        },
                    ),
                    (
                        id2,
                        NolerActor {
                            peer_ids: vec![id0, id1, id3, id4],
                        },
                    ),
                    (
                        id3,
                        NolerActor {
                            peer_ids: vec![id0, id1, id2, id4],
                        },
                    ),
                    (
                        id4,
                        NolerActor {
                            peer_ids: vec![id0, id1, id2, id3],
                        },
                    ),                    
                ],
            )
            .unwrap();
        }
        _ => {
            println!("USAGE:");
            println!("  ./noler check [CLIENT_COUNT] [NETWORK]");
            println!("  ./noler explore [CLIENT_COUNT] [ADDRESS] [NETWORK]");
            println!("  ./noler spawn");
            println!(
                "NETWORK: {}",
                Network::<<NolerActor as Actor>::Msg>::names().join(" | ")
            );
        }
    }

    Ok(())
}