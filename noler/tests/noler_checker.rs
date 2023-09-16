use noler::message::ElectionType;
use noler::constants::*;
//use noler::quorum::QuorumSet;
use noler::role::Role;
use noler::leaderelection_tester::LeaderElectionTester;

use serde::{Deserialize, Serialize};
use stateright::actor::register::{RegisterActor, RegisterMsg, RegisterMsg::*};
use stateright::actor::{majority, model_peers, Actor, ActorModel, Id, Network, Out};
use stateright::report::WriteReporter;
use stateright::semantics::register::Register;
use stateright::util::{HashableHashMap, HashableHashSet};
use stateright::{Checker, Expectation, Model};
use std::borrow::Cow;
use std::time::Duration;
use std::net::{SocketAddrV4, Ipv4Addr};
use stateright::actor::{*, register::*};
// use stateright::actor::model_timeout;
use rand::Rng;

type Ballot = (u32, u64);
type RequestId = u64;
type Value = char;

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, Hash, PartialEq)]
pub struct RequestVoteMessage {
    id: Id,
    replica_role: Role,
    ballot: (u32, u64),
    election_type: ElectionType,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, Hash, PartialEq)]
pub struct ResponseVoteMessage {
    pub ballot: (u32, u64),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Clone)]
pub struct HeartBeatMessage {
    pub ballot: (u32, u64),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct ConfigMessage {

}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Clone)]
pub struct RequestConfigMessage {
    pub ballot: (u32, u64),
}


#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Clone)]
pub struct PollLeaderMessage {
    pub ballot: (u32, u64),
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct PollLeaderOkMessage {
    pub ballot: (u32, u64),
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, Hash, PartialEq)]
enum NolerMsg {
    RequestVote(RequestVoteMessage),
    ResponseVote(ResponseVoteMessage),
    Config(ConfigMessage),
    HeartBeat(HeartBeatMessage),
    RequestConfig(RequestConfigMessage),
    PollLeader(PollLeaderMessage),
    PollLeaderOk(PollLeaderOkMessage),

    Prepare {
        ballot: Ballot,
    },

    Prepared {
        ballot: Ballot,
        last_accepted: Option<(Ballot, (RequestId, u32, Value))>,
    },
}


#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
enum ElectionTimer {
    LeaderInitTimeout,
    LeadershipVoteTimeout,
    LeaderVoteTimeout,
}

use NolerMsg::*;
use ElectionTimer::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NolerState {
    role: Role,
    status: u8,
    ballot: Ballot,
    voted: (Option<Id>, Option<Ballot>),
    leader: Option<u32>, 
    //leadership_quorum: QuorumSet<(u32, u32), ResponseVoteMessage>,

    accepted: Option<(Ballot, (RequestId, u32, Value))>,
    is_decided: bool,
}

#[derive(Clone, Debug)]
struct NolerActor {
    peer_ids: Vec<Id>,
}


impl Actor for NolerActor {
    type Msg = RegisterMsg<RequestId, Value, NolerMsg>;
    type State  = NolerState;
    type Timer = ElectionTimer;

    fn name(&self) -> String {
        "Noler Replica".to_owned()
    }

    fn on_start(&self, _id: Id, o: &mut Out<Self>) -> Self::State {

        let init_timer = rand::thread_rng().gen_range(1..5);

        o.set_timer(LeaderInitTimeout,
            Duration::from_secs(init_timer)..Duration::from_secs(init_timer));
        o.set_timer(LeaderVoteTimeout,
            Duration::from_secs(LEADER_VOTE_TIMEOUT)..Duration::from_secs(LEADER_VOTE_TIMEOUT));
        o.set_timer(LeadershipVoteTimeout,
            Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT)..Duration::from_secs(LEADERSHIP_VOTE_TIMEOUT));

        NolerState {
            role: Role::Member,
            status: INITIALIZATION,
            ballot: (0, 0),
            voted: (None, None),
            leader: None,
            //leadership_quorum: majority(self.peer_ids.clone()),

            accepted: None,
            is_decided: false,
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        if state.is_decided {
            if let Get(request_id) = msg {
                let (_b, (_req_id, _src, value)) =
                    state.accepted.expect("decided state has no accepted value");
            };
            return;
        }

        match msg {
            Put(request_id, value) => {
                let state = state.to_mut();

                state.ballot = (state.ballot.0 + 1, state.ballot.1 + 1);

                o.broadcast(
                    &self.peer_ids,
                    &Internal(Prepare {
                        ballot: state.ballot,
                    })
                )
            }

            Internal(Prepare { ballot }) if state.ballot < ballot => {
                state.to_mut().ballot = ballot;

                o.send(
                    src,
                    Internal(Prepared {
                        ballot,
                        last_accepted: state.accepted,
                    }),
                );

            }

            Internal(RequestVote (RequestVoteMessage {id, replica_role, ballot, election_type })) => {
                let state = state.to_mut();

                if state.ballot < ballot {
                    state.ballot = ballot;

                    state.voted = (Some(id), Some(state.ballot));

                    // o.send(
                    //     src,
                    //     Internal(ResponseVote( {
                    //         ballot: state.ballot,
                    //     })),
                    // );
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
            match timer {

               LeaderInitTimeout => {
                    //o.set_timer(LeaderInitTimeout, model_timeout());

                    for &dst in &self.peer_ids {
                        o.send(dst,
                            Internal(RequestVote (RequestVoteMessage {
                                id: id,
                                replica_role: Role::Member,
                                ballot: state.ballot,
                                election_type: ElectionType::Normal,
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
                                ballot: state.ballot,
                                election_type: ElectionType::Timeout,
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
                                ballot: state.ballot,
                                election_type: ElectionType::Timeout,
                            }))
                        );
                    }
                }
            }
    }
}

#[derive(Clone, Debug)]
struct NolerModelCfg {
    peer_count: usize,
    network: Network<<NolerActor as Actor>::Msg>,
}

impl NolerModelCfg {
    fn into_model(
        self,
    ) -> ActorModel<RegisterActor<NolerActor>, Self, LeaderElectionTester<Id, Register<Value>>>
    {
        ActorModel::new(
            self.clone(),
            LeaderElectionTester::new(Register(Value::default())),
        )
        .actors((0..self.peer_count).map(|i| {
            RegisterActor::Server(NolerActor {
                peer_ids: model_peers(i, self.peer_count),
            })
        }))
        .init_network(self.network)
        .property(Expectation::Always, "leader is elected", |_, state| {
            //state.history.is_some()
            true
        })
        // .property(Expectation::Always, "Leader is unique", |_, state| {
        //     //Loop through the states and ensure it is the same leader
        //     for s in state {
        //         if s.leader.is_some() {
        //             for s2 in state {
        //                 if s2.leader.is_some() {
        //                     if s.leader != s2.leader {
        //                         return false;
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // })
        // .property(Expectation::Sometimes, "leader is elected", |_, state| {
        //     state.leader.is_some()
        // })
       //.record_msg_in(RegisterMsg::record_returns) // client ops
        //.record_msg_out(RegisterMsg::record_invocations) // client ops
    }
}




#[cfg(test)]
#[test]
fn can_model_noler(){
    use stateright::actor::ActorModelAction::Deliver;

    let checker = NolerModelCfg {
        peer_count: 5,
        network: Network::new_unordered_nonduplicating([]),
    }
    .into_model()
    .checker()
    .spawn_bfs()
    .join();

    checker.assert_properties();
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