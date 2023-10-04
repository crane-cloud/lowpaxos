// use checker::election_tester::ElectionTester;
use checker::semantics::Election;
use noler::message::ElectionType;
use noler::constants::*;
use noler::role::Role;
//use noler::leaderelection_tester::LeaderElectionTester;
use noler::monitor::{Profile, NolerMonitorMatrix};
use noler::config::{ConfigSr, ReplicaSr};
// use checker::logsr::{LogSR, LogEntryState};
// use checker::kvstoresr::KVStoreSR;

use stateright::actor::register::{RegisterActor, RegisterMsg, RegisterMsg::*};
//use stateright::semantics::register::{RegisterActor, RegisterMsg, RegisterMsg::*};
use stateright::actor::{majority, model_peers, Actor, ActorModel, Id, Network, Out};
use stateright::report::WriteReporter;
// use stateright::semantics::register::Register;
// use stateright::semantics::LinearizabilityTester;
use stateright::util::HashableHashMap;
use stateright::{Checker, Expectation, Model};
use core::panic;
use std::borrow::Cow;
use std::time::Duration;
use std::net::{SocketAddrV4, Ipv4Addr};
use stateright::actor::spawn;
use rand::Rng;

use checker::election_actor::{ElectionActor, ElectionMsg};
use checker::leaderelection_tester::LeaderElectionTester;

type Ballot = (u32, u64);
type RequestId = u64;
type Value = char;
// type Request = (RequestId, Id, Option<Value>);

type NolerValue = (Role, ElectionType, u8); //KV Operation (key, option<value>)

// use checker::noler_msg_checker::NolerMsg::*;
use checker::noler_msg_checker::ElectionTimer::*;
// use checker::noler_msg_checker::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NolerElectionState {
    role: Role,
    status: u8,
    ballot: Ballot,
    voted: Option<(Id, Ballot, ElectionType)>,
    leader: Option<(Id, Ballot)>,
    leadership_quorum: HashableHashMap<Id, Option<Ballot>>,
    monitor: NolerMonitorMatrix,
    config: ConfigSr,
}

#[derive(Clone, Debug)]
struct NolerElectionActor {
    peer_ids: Vec<Id>,
}


impl Actor for NolerElectionActor {
    //type Msg = RegisterMsg<RequestId, Value, checker::noler_msg_checker::NolerMsg>;\
    type Msg = ElectionMsg<Ballot, NolerValue, ConfigSr, checker::noler_msg_checker::NolerMsg>;
    //type Msg = RegisterMsg<RequestId, ValueNoler, noler::noler_msg_checker::NolerMsg>;
    type State  = NolerElectionState;
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

                let profile = Profile::new(rand::thread_rng().gen_range(0..100));

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

        NolerElectionState {
            role: Role::Member,
            status: INITIALIZATION,
            ballot: (0, 0),
            voted: None,
            leader: None,
            leadership_quorum: Default::default(),
            monitor: matrix,
            config: ConfigSr::new((0, 0), n, replicas),
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
            ElectionMsg::RequestVote(ballot, (_replica_role, election_type, profile)) => {
                let state = state.to_mut();

                /////////////////////////////////////////////////////////////////
                //*First-time election request*//
                if state.voted.is_none() && state.leader.is_none() {
                    o.cancel_timer(LeaderInitTimeout);

                    if state.ballot < ballot {
                        if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                            if profile >= state.monitor.get(&src).unwrap().get_x() {
                                state.voted = Some((src, ballot, election_type));
                                o.send(
                                    src,
                                    ElectionMsg::ResponseVote(ballot)
                                );

                                //Start the leadership vote timeout
                                o.set_timer(LeadershipVoteTimeout, 
                                    Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));
                            }

                            else {
                                state.role = Role::Candidate;

                                //Start the leader vote timeout
                                o.set_timer(LeaderVoteTimeout, 
                                    Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                            }
                        }

                        else {
                            state.voted = Some((src, ballot, election_type));

                            o.send(
                                src,
                                ElectionMsg::ResponseVote(ballot),
                            );
                        }
                    }

                    else { 
                        log::info!("{} has a higher ballot", idx);
                    }
                }
                /////////////////////////////////////////////////////////////////
                //*Replica has voted before - but has no leader information */
                else if state.voted.is_some() && state.leader.is_none() {

                    if ballot == state.voted.unwrap().1 && src == state.voted.unwrap().0 && election_type == state.voted.unwrap().2 {
                        //duplicate vote request - resend response
                        o.send(
                            src,
                            ElectionMsg::ResponseVote(ballot),
                        );

                        return;
                    }

                    if ballot < state.voted.unwrap().1 {
                        log::info!("{}: ballot is lower than voted ballot", idx);
                        return;
                    }


                    if ballot == state.voted.unwrap().1 {
                        //log::info!("{}: ballot is the same as voted ballot", idx);

                        match election_type {
                            ElectionType::Timeout => if state.voted.unwrap().2 == ElectionType::Normal || state.voted.unwrap().2 == ElectionType::Degraded {
                                match state.role { 
                                    Role::Member => {
                                        //Process the vote immediately - earlier request had better profile
                                        state.voted = Some((src, ballot, election_type));

                                        o.send(
                                            src,
                                            ElectionMsg::ResponseVote(ballot),
                                        );
                                    }
                                    Role::Candidate => {
                                        //Process the vote - still an initial election
                                        if profile >= state.monitor.get(&src).unwrap().get_x() {
                                            state.voted = Some((src, ballot, election_type));
                                            o.send(
                                                src,
                                                ElectionMsg::ResponseVote(ballot)
                                            );
                                        }

                                        else {
                                            //Start the leadership vote timer
                                            o.set_timer(LeadershipVoteTimeout, 
                                                Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));

                                        }
                                    }
                                    _ => {
                                        log::info!("{}: only member and candidate can process timeout election", idx);
                                        return;
                                    }
                                }

                            }
                            ElectionType::Degraded => if state.voted.unwrap().2 == ElectionType::Normal || state.voted.unwrap().2 == ElectionType::Timeout {
                                //Provide a response immediately
                                state.voted = Some((src, ballot, election_type));

                                o.send(
                                    src,
                                    ElectionMsg::ResponseVote(ballot),
                                );

                            }

                            _ => {
                                log::info!("{}: only timeout & degraded elections allowed - received {:?} and ballot {:?}", idx, election_type, ballot);
                                return;
                            }
                        }

                    }


                    else if ballot > state.voted.unwrap().1 {
                        //Provide a vote response immediately - the request ballot is much higher (a leader must have been elected before)
                        log::info!("{}: vote as request ballot > seen before and we know no leader", idx);
                        state.voted = Some((src, ballot, election_type));
                    }

                }

                /////////////////////////////////////////////////////////////////
                //*Replica has never voted before - but has leader information */
                else if state.voted.is_none() && state.leader.is_some() {
                    log::info!("{} has a leader with ballot {:?}", idx, state.ballot);

                    //Check that the leader ballot is higher than the request ballot
                    if state.leader.unwrap().1 >= ballot || state.ballot >= ballot {
                        log::info!("{} has a higher leader|own ballot", idx);
                        return;
                    }

                    else {
                        match election_type {
                            ElectionType::Profile => if state.role != Role::Leader {
                                match state.role {

                                    Role::Candidate => {
                                        if (ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1) ||
                                            (ballot.0 == state.leader.unwrap().1.0 + 1 && ballot.1 == state.leader.unwrap().1.1 + 1) {

                                            if profile >= state.monitor.get(&src).unwrap().get_x() {
                                                state.voted = Some((src, ballot, election_type));
                                                o.send(
                                                    src,
                                                    ElectionMsg::ResponseVote(ballot)
                                                );
                
                                                //Start the leadership vote timeout
                                                o.set_timer(LeadershipVoteTimeout, 
                                                    Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));
                                            }
                
                                            else {
                
                                                //Start the leader vote timeout
                                                o.set_timer(LeaderVoteTimeout, 
                                                    Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                            }
                                        }

                                        else {
                                            //Provide a response immediately as the request ballot is much higher
                                            state.voted = Some((src, ballot, election_type));
                                            o.send(
                                                src, 
                                                ElectionMsg::ResponseVote(ballot)
                                            );
                                        }
                                    },

                                    Role::Witness => {
                                        //Provide a vote immediately
                                        state.voted = Some((src, ballot, election_type));
                                        o.send(
                                            src, 
                                            ElectionMsg::ResponseVote(ballot)
                                        );
                                    },

                                    _ => {
                                        log::info!("{}: only witness & candidate can participate", idx);
                                        return;
                                    }
                                }
                            },
                            ElectionType::Offline => if state.role != Role::Leader {
                                match state.role {

                                    Role::Candidate => {
                                        if (ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1) ||
                                            (ballot.0 == state.leader.unwrap().1.0 + 1 && ballot.1 == state.leader.unwrap().1.1 + 1) {

                                            if profile >= state.monitor.get(&src).unwrap().get_x() {
                                                state.voted = Some((src, ballot, election_type));
                                                o.send(
                                                    src,
                                                    ElectionMsg::ResponseVote(ballot)
                                                );
                
                                                //Start the leadership vote timeout
                                                o.set_timer(LeadershipVoteTimeout, 
                                                    Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));
                                            }
                
                                            else {
                
                                                //Start the leader vote timeout
                                                o.set_timer(LeaderVoteTimeout, 
                                                    Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                            }
                                        }

                                        else {
                                            //Provide a response immediately as the request ballot is much higher
                                            state.voted = Some((src, ballot, election_type));
                                            o.send(
                                                src, 
                                                ElectionMsg::ResponseVote(ballot)
                                            );
                                        }
                                    },

                                    Role::Witness => {
                                        //Provide a vote immediately
                                        state.voted = Some((src, ballot, election_type));
                                        o.send(
                                            src, 
                                            ElectionMsg::ResponseVote(ballot)
                                        );
                                    },

                                    _ => {
                                        log::info!("{}: only candidate & witness participate", idx);
                                        return;
                                    }
                                }
                            },

                            _ => {
                                log::info!("{}: only profile and offline election allowed", idx);
                                return;
                            }
                        }
                    }

                }

 
                //*Replica has voted & leader information*//
                else if state.voted.is_some() && state.leader.is_some() {

                    if ballot <= state.voted.unwrap().1 || ballot <= state.leader.unwrap().1 {
                        log::info!("{}: ballot is lower than voted or leader ballot", idx);
                        return
                    }

                    //Check if the ballot is the same as the voted ballot and voted
                    if ballot == state.voted.unwrap().1 && src == state.voted.unwrap().0 && election_type == state.voted.unwrap().2 {
                        //duplicate vote request - resend response
                        o.send(
                            src,
                            ElectionMsg::ResponseVote(ballot),
                        );

                        return;
                    }

                    match election_type {
                        ElectionType::Normal => {
                            log::info!("{}: normal election request not allowed here", idx);
                            return;
                        }

                        ElectionType::Profile => {
                            match state.role {
                                Role::Leader => {
                                    if profile > state.monitor.get(&src).unwrap().get_x() {
                                        log::info!("{}: new member {} with better profile seeks election", idx, src);

                                        state.status = ELECTION;

                                        state.role = Role::Candidate;

                                        state.voted = Some((src, ballot, election_type));

                                        o.send(
                                            src,
                                            ElectionMsg::ResponseVote(ballot),
                                        );

                                        o.set_timer(LeaderVoteTimeout, 
                                            Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
                                    }

                                    else {
                                        log::info!("{}: new member {} with worse profile seeks election", idx, src);
                                        //affirm leadership
                                        state.ballot = (state.ballot.0 + 1, state.ballot.1 + 1);

                                        //update the config
                                        let mut config = state.config.clone();

                                        let role_mapping = vec![
                                            //(0, 0, Role::Leader),
                                            (0, ((config.n/2) - 1) , Role::Candidate),
                                            ((config.n)/2, (config.n - 1), Role::Witness),
                                        ];

                                        for replica in config.replicas.iter_mut() {
                                            if src == idx {
                                                replica.profile = 100;
                                            }

                                            else {
                                                replica.profile = state.monitor.get(&src).unwrap().get_x();
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
                                            &ElectionMsg::Config(ballot, config.clone()),
                                        );

                                    }
                                }
                                Role::Candidate => {
                                    if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                                        if profile >= state.monitor.get(&src).unwrap().get_x() {
                                            state.voted = Some((src, ballot, election_type));
                                            o.send(
                                                src,
                                                ElectionMsg::ResponseVote(ballot)
                                            );
                                        }

                                        else {

                                            o.set_timer(LeadershipVoteTimeout, 
                                                Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));
                                        }
                                    }

                                    else {
                                        //Provide a response immediately as the request ballot is much higher
                                        state.voted = Some((src, ballot, election_type));
                                        o.send(
                                            src, 
                                            ElectionMsg::ResponseVote(ballot)
                                        );
                                    }
                                }
                                Role::Witness => {
                                    //Provide a vote immediately
                                    state.voted = Some((src, ballot, election_type));
                                    o.send(
                                        src, 
                                        ElectionMsg::ResponseVote(ballot)
                                    );
                                }
                                Role::Member => {
                                    log::info!("{}: member can't have leader information", idx);
                                }
                            }
                        }

                        ElectionType::Offline => {
                            match state.role {
                                Role::Leader => {
                                    //affirm leadership
                                    state.ballot = (state.ballot.0 + 1, state.ballot.1 + 1);

                                    //update the config
                                    let mut config = state.config.clone();

                                    let role_mapping = vec![
                                            //(0, 0, Role::Leader),
                                            (0, ((config.n/2) - 1) , Role::Candidate),
                                            ((config.n)/2, (config.n - 1), Role::Witness),
                                    ];

                                    for replica in config.replicas.iter_mut() {
                                        if src == idx {
                                            replica.profile = 100; //Not in the config.replicas as it is the same host
                                        }

                                        else {
                                            replica.profile = state.monitor.get(&src).unwrap().get_x();
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
                                        &ElectionMsg::Config(ballot, config.clone()),
                                    );
                                }

                                Role::Candidate => {
                                    if ballot.0 == state.ballot.0 + 1 && ballot.1 == state.ballot.1 + 1 {

                                        if profile >= state.monitor.get(&src).unwrap().get_x() {
                                            state.voted = Some((src, ballot, election_type));
                                            o.send(
                                                src,
                                                ElectionMsg::ResponseVote(ballot)
                                            );
                                        }

                                        else {
                                            o.set_timer(LeadershipVoteTimeout, 
                                                Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));
                                        }
                                    }

                                    else {
                                        //Provide a response immediately as the request ballot is much higher
                                        state.voted = Some((src, ballot, election_type));
                                        o.send(
                                            src, 
                                            ElectionMsg::ResponseVote(ballot)
                                        );
                                    }
                                }

                                Role::Witness => {
                                    //Provide a vote immediately
                                    state.voted = Some((src, ballot, election_type));
                                    o.send(
                                        src, 
                                        ElectionMsg::ResponseVote(ballot)
                                    );
                                }

                                Role::Member => {
                                    log::info!("{}: member can't have leader information", idx);
                                    return;
                                }

                            }    

                        }

                        ElectionType::Timeout => if state.role == Role::Candidate || state.role == Role::Witness { //A leader can only start this election type after a candidate fails to attain leadership
                            //Candidates/Witnesses vote immediately
                            state.voted = Some((src, ballot, election_type));
                            o.send(
                                src, 
                                ElectionMsg::ResponseVote(ballot)
                            );
                        }

                        ElectionType::Degraded => if state.role == Role::Candidate || state.role == Role::Witness { //A candidate may start this election - leader has not been assertive or one of the candidates not been successful
                            //Candidates/Witnesses vote immediately
                            state.voted = Some((src, ballot, election_type));
                            o.send(
                                src, 
                                ElectionMsg::ResponseVote(ballot)
                            );
                        }
                    }
                }
            }

            ElectionMsg::ResponseVote(ballot) if ballot == (state.ballot.0 + 1, state.ballot.1 + 1) => {
                let state = state.to_mut();
                
                log::info!("{} now checking for quorum....", idx);

                state.leadership_quorum.insert(src, Some(ballot));

                //log::info!("{} has {:?} votes", idx, state.leadership_quorum.len());
                    
                if state.leadership_quorum.len() == majority(self.peer_ids.len() + 1) {
                    
                    //print the votes
                    for (k, v) in state.leadership_quorum.iter() {
                        log::info!("{}: {:?} - {:?}", idx, k, v);
                    }

                    state.status = NORMAL;
                    state.role = Role::Leader;
                    state.ballot = ballot;
                    state.leader = Some((idx, ballot));


                    let mut config = state.config.clone();
                    //Update the config with leader information
                    config.leader = idx;

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
                        &ElectionMsg::Config(ballot, config.clone()),
                    );

                    //Start the leader lease timeout
                    o.set_timer(LeaderLeaseTimeout, 
                        Duration::from_secs(LEADER_LEASE_TIMEOUT)..Duration::from_secs(LEADER_LEASE_TIMEOUT));

                }

                else {
                    log::info!("{} is still waiting for quorum....", idx);
                }

            }

            ElectionMsg::Config(ballot, config) if state.role != Role::Leader => {
                let state = state.to_mut();

                o.cancel_timer(LeadershipVoteTimeout);
                o.cancel_timer(LeaderVoteTimeout);
                o.cancel_timer(LeaderInitTimeout);

                if config.ballot > state.ballot {
                    state.config = config.clone();
                    state.leader = Some((config.leader, ballot));
                    state.ballot = ballot;

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

                            //Start the poll leader timer & poll leader timeout
                            o.set_timer(PollLeaderTimer, 
                                Duration::from_secs(POLL_LEADER_TIMER)..Duration::from_secs(POLL_LEADER_TIMER));

                            o.set_timer(PollLeaderTimeout, 
                                Duration::from_secs(POLL_LEADER_TIMEOUT)..Duration::from_secs(POLL_LEADER_TIMEOUT));

                            //Start the heartbeat timer & heartbeat timeout
                            o.set_timer(HeartBeatTimeout, 
                                Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));   
                        }

                        Role::Witness => {
                            //change status to normal
                            state.status = NORMAL;
                            log::info!("{}: now a witness - start timers", idx);
                            o.set_timer(HeartBeatTimeout,
                                Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));
                        }

                        Role::Member => {
                            log::info!("{}: config can't have a member role", idx);
                            return;
                        }
                    }
                }

                else {
                    log::info!("{}: config ballot is lower than current ballot", idx);
                    panic!("{}: config ballot is lower than current ballot - 2 leaders", idx);
                    //return;
                    //assert_eq!(config.ballot, state.ballot, "config ballot is lower than current ballot - 2 leaders");
                }
            }

            ElectionMsg::RequestConfig(ballot) if state.status == NORMAL && state.role == Role::Leader => {
                let state = state.to_mut();

                if ballot > state.ballot {
                    log::info!("{}: received ballot is greater than current ballot - stale leader", idx);
                    return;
                }

                else {
                    o.send(src, ElectionMsg::Config(ballot, state.config.clone()));
                }
            }

            ElectionMsg::HeartBeat(ballot, profile) if state.status == NORMAL && (state.role == Role::Candidate || state.role == Role::Witness) => {
                let state = state.to_mut();

                if ballot >= state.ballot {
                    match state.role {
                        Role::Candidate => {

                            let p1 = state.monitor.get(&src).unwrap().get_x();
                            let p2 = profile;

                            if (p2 - p1) > 51 {
                                //Cancel the candidate-user timers
                                o.cancel_timer(PollLeaderTimer);
                                o.cancel_timer(PollLeaderTimeout);
                                o.cancel_timer(HeartBeatTimeout);

                                //Start an election with type profile

                                state.status = ELECTION;

                                state.voted = Some((idx, ballot, ElectionType::Profile));

                                for &dst in &self.peer_ids {
                                    if idx == dst {
                                        continue;
                                    }
                                    o.send(dst,
                                        ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                            (state.role, ElectionType::Profile, {
                                                    if let Some(profile) = state.monitor.get(&dst) {
                                                        profile.get_x()
                                                    }
                                                    else { 0 }
                                            }))
                                    );
                                }
                            }

                            else {
                                //Restart all candidate-use timers
                                o.cancel_timer(PollLeaderTimer);
                                o.cancel_timer(PollLeaderTimeout);
                                o.cancel_timer(HeartBeatTimeout);

                                o.set_timer(PollLeaderTimer, 
                                    Duration::from_secs(POLL_LEADER_TIMER)..Duration::from_secs(POLL_LEADER_TIMER));
                                o.set_timer(PollLeaderTimeout, 
                                    Duration::from_secs(POLL_LEADER_TIMEOUT)..Duration::from_secs(POLL_LEADER_TIMEOUT));
                                o.set_timer(HeartBeatTimeout, 
                                    Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));
                            }
                        }
                        Role::Witness => {

                            let p1 = state.monitor.get(&src).unwrap().get_x();
                            let p2 = profile;

                            if (p2 - p1) > 60 {
                                //Cancel the witness-user timers
                                o.cancel_timer(HeartBeatTimeout);

                                //Start a new election
                                state.status = ELECTION;

                                state.voted = Some((idx, ballot, ElectionType::Degraded));

                                for &dst in &self.peer_ids {
                                    if idx == dst {
                                        continue;
                                    }
                                    o.send(dst,
                                        ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                            (state.role, ElectionType::Profile, {
                                                    if let Some(profile) = state.monitor.get(&dst) {
                                                        profile.get_x()
                                                    }
                                                    else { 0 }
                                            }))
                                    );
                                }
                            }
                            //Cancel and start the heartbeat timeout
                            o.cancel_timer(HeartBeatTimeout);

                            o.set_timer(HeartBeatTimeout, 
                                Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));
                        }
                        _ => {}
                    }
                }

                else {
                    // Request for config from the leader ** Leader may always be ahead
                    o.send(state.leader.unwrap().0, ElectionMsg::RequestConfig(ballot));

                    return;
                }
            }

            ElectionMsg::PollLeader(ballot) if state.status == NORMAL && state.role == Role::Leader => {
                let state = state.to_mut();

                if ballot <= state.ballot {
                    o.send(src, ElectionMsg::PollLeaderOk(ballot, state.monitor.get(&src).unwrap().get_x()));
                }

                else {
                    log::info!("{}: poll leader ballot is higher than current ballot - stale leader", idx);
                }

            }

            ElectionMsg::PollLeaderOk(ballot, profile) if state.status == NORMAL && state.role == Role::Candidate => {
                let state = state.to_mut();

                if ballot >= state.ballot {

                    let p1 = state.monitor.get(&src).unwrap().get_x();
                    let p2 = profile;

                    if (p2 - p1) > 51 {
                        //Stop the candidate-role timers
                        o.cancel_timer(PollLeaderTimer);
                        o.cancel_timer(PollLeaderTimeout);
                        o.cancel_timer(HeartBeatTimeout);

                        //Start an election with type profile

                        state.status = ELECTION;

                        state.voted = Some((idx, ballot, ElectionType::Profile));

                        for &dst in &self.peer_ids {
                            if idx == dst {
                                continue;
                           }
                            o.send(dst,
                                ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                    (state.role, ElectionType::Profile, {
                                            if let Some(profile) = state.monitor.get(&dst) {
                                                profile.get_x()
                                            }
                                            else { 0 }
                                    }))
                            );
                        }
                    }

                    else {
                        //Cancel & start all the timers
                        o.cancel_timer(PollLeaderTimer);
                        o.cancel_timer(PollLeaderTimeout);
                        o.cancel_timer(HeartBeatTimeout);

                        o.set_timer(PollLeaderTimer, 
                            Duration::from_secs(POLL_LEADER_TIMER)..Duration::from_secs(POLL_LEADER_TIMER));
                        o.set_timer(PollLeaderTimeout, 
                            Duration::from_secs(POLL_LEADER_TIMEOUT)..Duration::from_secs(POLL_LEADER_TIMEOUT));
                        o.set_timer(HeartBeatTimeout, 
                            Duration::from_secs(HEARTBEAT_TIMEOUT)..Duration::from_secs(HEARTBEAT_TIMEOUT));
                    }
                }

                else {
                    log::info!("{}: PLO ballot is lower than current ballot - stale leader?", idx); //New election
                }
            }

            _ => {}

        }
    }

    fn on_timeout(
            &self,
            id: Id,
            state: &mut Cow<Self::State>,
            timer: &Self::Timer,
            o: &mut Out<Self>,
        ) {
            let state = state.to_mut();
            match timer {

               LeaderInitTimeout => if state.role == Role::Member  {
                    o.cancel_timer(LeaderInitTimeout);
                    state.voted = Some((id, (state.ballot.0 + 1, state.ballot.1 + 1), ElectionType::Normal));
                    state.status = ELECTION;
                    for &dst in &self.peer_ids {
                        if id == dst {
                             continue;
                        }
                        o.send(dst,
                            ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                (state.role, ElectionType::Normal, {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }))
                        );
                    }
                }
                LeadershipVoteTimeout => if state.role == Role::Candidate || state.role == Role::Member {
                    //o.set_timer(LeadershipVoteTimeout, model_timeout());
                    o.cancel_timer(LeadershipVoteTimeout);
                    state.voted = Some((id, (state.ballot.0 + 1, state.ballot.1 + 1), ElectionType::Timeout));
                    state.status = ELECTION;
                    for &dst in &self.peer_ids {
                        if id == dst {
                            continue;
                       }
                        o.send(dst,
                            ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                (state.role, ElectionType::Degraded, {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }))
                        );
                    }

                }

                LeaderVoteTimeout => if state.role == Role::Candidate {
                    o.cancel_timer(LeaderVoteTimeout);
                    //Update the voted with itself
                    state.status = ELECTION;
                    state.voted = Some((id, (state.ballot.0 + 1, state.ballot.1 + 1), ElectionType::Timeout));

                    for &dst in &self.peer_ids {
                        if id == dst {
                            continue;
                       }
                        o.send(dst,
                            ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                (state.role, ElectionType::Timeout, {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }))
                        );
                    }
                }

                PollLeaderTimeout => if state.role == Role::Candidate {
                    log::info!("{}: received a poll leader timeout", id);
                    o.cancel_timer(PollLeaderTimeout);
                    //Update the voted with itself
                    state.status = ELECTION;
                    state.voted = Some((id, (state.ballot.0 + 1, state.ballot.1 + 1), ElectionType::Timeout));

                    for &dst in &self.peer_ids {
                        if id == dst {
                            continue;
                       }
                        o.send(dst,
                            ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                (state.role, ElectionType::Offline, {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }))
                        );
                    }
                }

                HeartBeatTimeout => if state.role == Role::Candidate || state.role == Role::Witness {
                    log::info!("{}: received a heartbeat timeout", id);
                    o.cancel_timer(HeartBeatTimeout);
                    //Update the voted with itself
                    state.status = ELECTION;
                    state.voted = Some((id, (state.ballot.0 + 1, state.ballot.1 + 1), ElectionType::Timeout));

                    for &dst in &self.peer_ids {
                        if id == dst {
                            continue;
                       }
                        o.send(dst,
                            ElectionMsg::RequestVote((state.ballot.0 + 1, state.ballot.1 + 1), 
                                (state.role, ElectionType::Offline, {
                                        if let Some(profile) = state.monitor.get(&dst) {
                                            profile.get_x()
                                        }
                                        else { 0 }
                                }))
                        );
                    }
                }

                PollLeaderTimer => if state.role == Role::Candidate {
                    log::info!("{}: received a poll leader timer", id);
                    o.cancel_timer(PollLeaderTimer);
    
                    //Send the poll leader message to the leader
                    o.send(state.leader.unwrap().0,
                        ElectionMsg::PollLeader(state.ballot)
                    );
                }
    
                LeaderLeaseTimeout => if state.role == Role::Leader {
                    log::info!("{}: received a leader lease timeout", id);
                    o.cancel_timer(LeaderLeaseTimeout);
    
                    for &dst in &self.peer_ids {
                        if id == dst {
                            continue;
                       }
                       o.send(dst,
                        ElectionMsg::HeartBeat(state.ballot, state.monitor.get(&dst).unwrap().get_x())
                        );
                    }
                    o.set_timer(LeaderLeaseTimeout, 
                        Duration::from_secs(LEADER_LEASE_TIMEOUT)..Duration::from_secs(LEADER_LEASE_TIMEOUT));
                }
            }

    }


}

#[derive(Clone, Debug)]
struct NolerElectionModelCfg {
    //client_count: usize,
    peer_count: usize,
    config: ConfigSr,
    network: Network<<NolerElectionActor as Actor>::Msg>,
}

impl NolerElectionModelCfg {
    fn new(peer_count: usize, start_port: u16, network: Network<<NolerElectionActor as Actor>::Msg>) -> Self {
        let mut replicas = Vec::new();

        for i in 0..peer_count {
            let port = start_port + i as u16;
            let replica = ReplicaSr {
                id: Id::from(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
                status: INITIALIZATION,
                role: Role::new(),
                profile: 0,
            };
            replicas.push(replica);
        }

        let config = ConfigSr::new((0, 0), peer_count, replicas);

        NolerElectionModelCfg {
            //client_count: 1,
            peer_count,
            config,
            network,
        }

    }

    fn into_model(
        self,
    ) -> ActorModel<
        ElectionActor<NolerElectionActor>,
        Self,
        LeaderElectionTester<Id, Election<Ballot, NolerValue, ConfigSr>>>
    {
        //let nv_default = (Role::Member, ElectionType::Normal, 0);

        ActorModel::new(
            self.clone(),
            // LinearizabilityTester::new(Register(Value::default())),
            LeaderElectionTester::new(Election((0, 0), (Role::Member, ElectionType::Normal, 0), self.config.clone())),
        )
        .actors((0..self.peer_count).map(|i| {
            ElectionActor::Server(NolerElectionActor {
                peer_ids: model_peers(i, self.peer_count),
            })
        }))
        .init_network(self.network)
        .property(Expectation::Sometimes, "leader elected sometimes", |_, state| {
            state.network.iter_deliverable().any(|env| {
                if let ElectionMsg::Config(ballot, config) = env.msg {
                    let leader = config.leader;
                    log::info!("Leader elected: {} with ballot {:?}", leader, ballot);
                    true
                } else {
                    false
                }
            })
        })
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
        // .property(Expectation::Sometimes, "value chosen", |_, state| {
        //     for env in state.network.iter_deliverable() {
        //         if let RegisterMsg::GetOk(_req_id, value) = env.msg {
        //             if *value != Value::default() {
        //                 return true;
        //             }
        //         }
        //     }
        //     false
        // })
        .record_msg_in(ElectionMsg::record_returns)
        .record_msg_out(ElectionMsg::record_invocations)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn can_model_noler() {
        let peer_count = 5;
        let start_port = 3000;

        let checker = NolerElectionModelCfg::new(
            peer_count, start_port, Network::new_unordered_nonduplicating([]))
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

            let peer_count = args
                .opt_free_from_str()?
                .unwrap_or(5);

            let start_port = args
                .opt_free_from_str()?
                .unwrap_or(3000);

            println!("Model checking Noler with {} peers, network {:?} & ports starting from {}", peer_count, network, start_port);

            let check = NolerElectionModelCfg::new(peer_count, start_port, network);

            check
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

            let peer_count = args
                .opt_free_from_str()?
                .unwrap_or(5);

            let start_port = args
                .opt_free_from_str()?
                .unwrap_or(3000);

            println!("Exploring Noler state space with network {:?} and address {}: {} peers with start port {}.", network, address, peer_count, start_port);

            let explore = NolerElectionModelCfg::new(peer_count, start_port, network);

            explore
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
                serde_json::to_string(&RegisterMsg::Put::<RequestId, Value, ()>(1, 'X')).unwrap()
            );
            println!(
                "{}",
                serde_json::to_string(&RegisterMsg::Get::<RequestId, Value, ()>(2)).unwrap()
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
                        NolerElectionActor {
                            peer_ids: vec![id1, id2, id3, id4],
                        },
                    ),
                    (
                        id1,
                        NolerElectionActor {
                            peer_ids: vec![id0, id2, id3, id4],
                        },
                    ),
                    (
                        id2,
                        NolerElectionActor {
                            peer_ids: vec![id0, id1, id3, id4],
                        },
                    ),
                    (
                        id3,
                        NolerElectionActor {
                            peer_ids: vec![id0, id1, id2, id4],
                        },
                    ),
                    (
                        id4,
                        NolerElectionActor {
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
                Network::<<NolerElectionActor as Actor>::Msg>::names().join(" | ")
            );
        }
    }

    Ok(())
}