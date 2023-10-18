#[cfg(doc)]
use stateright::actor::ActorModel;
use stateright::actor::{Actor, Envelope, Id, Out};
// use stateright::semantics::register::{Register, RegisterOp, RegisterRet};
//use stateright::semantics::ConsistencyTester;
use crate::semantics::{NolerElection, NolerElectionOp, NolerElectionRet, 
                        Noler, NolerOp, NolerRet,
                        NolerStore, NolerStoreOp, NolerStoreRet
                    };

use crate::election_tester::ElectionTester;
use core::panic;
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::Hash;

use noler::config::ConfigSr;
use noler::role::Role;
use noler::message::ElectionType;

// #[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
// pub enum ElectionMsg<Ballot, NolerValue, ConfigSr, InternalMsg> {
//     Internal(InternalMsg),
//     //Request(RequestId),
//     RequestVote(Ballot, NolerValue),
//     RequestConfig(Ballot),
//     ResponseVote(Ballot),
//     Config(Ballot, ConfigSr),
//     HeartBeat(Ballot, u8),
//     PollLeader(Ballot),
//     PollLeaderOk(Ballot, u8),
// }


#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum NolerElectionMsg<Ballot, ConfigSR, InternalMsg> {
    Internal(InternalMsg),
    NolerRequestVote(Ballot, (Role, ElectionType, u8)),
    NolerResponseVote(Ballot),
    NoVote,
    NolerConfig(Ballot, ConfigSR),
    NolerHeartBeat(Ballot, u8),
    NolerPollLeader(Ballot),
    NolerPollLeaderOk(Ballot, u8),
    NolerRequestConfig(Ballot),
}

////////////////////////////////////////////////////////////////////////////////////////////////////
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum NolerMsg<Ballot, NolerValue, ConfigSr, InternalMsg> {
    Internal(InternalMsg),
    RequestVote(Ballot, NolerValue),
    ResponseVote(Ballot),
    Config(Ballot, ConfigSr),
    RequestConfig(Ballot),
    HeartBeat(Ballot, u8),
    PollLeader(Ballot),
    PollLeaderOk(Ballot, u8),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum KvStoreMsg<RequestId, Key, Value, InternalMsg> {
    Internal(InternalMsg),
    //Request(RequestId),
    Set(RequestId, Key, Value),
    SetOk(RequestId, Key),

    Get(RequestId, Key),
    GetOk(RequestId, Key, Value),
}


use NolerMsg::*;

impl <Ballot, NolerValue, ConfigSr, InternalMsg> NolerMsg <Ballot, NolerValue, ConfigSr, InternalMsg> {
    pub fn record_invocations<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&NolerMsg<Ballot, NolerValue, ConfigSr, InternalMsg>>,
    ) -> Option<H>
    where
        H: Clone + ElectionTester<Id, Noler<Ballot, ConfigSr>>,
        Ballot: Clone + Debug + PartialEq + Eq + PartialOrd,
        NolerValue: Clone + Debug + PartialEq,
        ConfigSr: Clone + Debug + PartialEq,
        //Role: Clone + Debug + PartialEq,
        //ConfigSr: Clone + Debug + PartialEq,
        //ElectionType: Clone + Debug + PartialEq,
    {
        match env.msg {
            RequestVote(b, _nv) => {
                let mut history = history.clone();
                let _ = history.on_invoke(env.src, NolerOp::Vote(b.clone()));
                Some(history)
            }

            _ => None,

        }

    }

    pub fn record_returns<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&NolerMsg<Ballot, NolerValue, ConfigSr, InternalMsg>>,
    ) -> Option<H>
    where
        H: Clone + ElectionTester<Id, Noler<Ballot, ConfigSr>>,
        Ballot: Clone + Debug + PartialEq + Eq + PartialOrd,
        NolerValue: Clone + Debug + PartialEq,
        ConfigSr: Clone + Debug + PartialEq,
        //Role: Clone + Debug + PartialEq,
        //ElectionType: Clone + Debug + PartialEq,
    {
        match env.msg {
            ResponseVote(b) => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, NolerRet::VoteOk(b.clone()));
                Some(history)
            }
            // NoVote => {
            //     let mut history = history.clone();
            //     let _ = history.on_return(env.dst, NolerRet::NoVote);
            //     Some(history)
            // }
            Config(b, c) => {
                let mut history = history.clone();
                //let config = c.clone();
                let _ = history.on_return(env.dst, NolerRet::Configuration(b.clone(), c.clone()));
                Some(history)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NolerActor<ServerActor> {
    Server(ServerActor),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub enum NolerActorState<ServerState> {
    Server(ServerState),
}

impl<ServerActor, InternalMsg> Actor for NolerActor<ServerActor>
where
    ServerActor: Actor<Msg = NolerMsg<(u32, u64), (Role, ElectionType, u8), ConfigSr, InternalMsg>>,
    InternalMsg: Clone + Debug + Eq + Hash,
{
    type Msg = NolerMsg<(u32, u64), (Role, ElectionType, u8), ConfigSr, InternalMsg>;
    type State = NolerActorState<ServerActor::State>;
    type Timer = ServerActor::Timer;

    fn name(&self) -> String {
        match self {
            NolerActor::Server(s) => {
                let n = s.name();
                if n.is_empty() {
                    "Server".to_owned()
                } else {
                    n
                }
            }
        }
    }

    #[allow(clippy::identity_op)]
    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        match self {
            NolerActor::Server(server_actor) => {
                let mut server_out = Out::new();
                let state = NolerActorState::Server(server_actor.on_start(id, &mut server_out));
                o.append(&mut server_out);
                state
            }
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut
        Out<Self>,
    ) {
        use NolerActor as A;
        use NolerActorState as S;

        match (self, &**state) {
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();

                server_actor.on_msg(id, &mut server_state, src, msg, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(NolerActorState::Server(server_state))
                }
                o.append(&mut server_out);                
            }
        }
    }

    fn on_timeout(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        use NolerActor as A;
        use NolerActorState as S;
        match (self, &**state) {
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                server_actor.on_timeout(id, &mut server_state, timer, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(NolerActorState::Server(server_state))
                }
                o.append(&mut server_out);
            }
        }
    }

}


use KvStoreMsg::*;

impl <RequestId, Key, Value, InternalMsg> KvStoreMsg<RequestId, Key, Value, InternalMsg> {
    pub fn record_invocations<C, H> (
        _cfg: &C,
        history: &H,
        env: Envelope<&KvStoreMsg<RequestId, Key, Value, InternalMsg>>,
    ) -> Option<H>
    where
        H: Clone + ElectionTester<Id, NolerStore<Key, Value>>,
        Key: Clone + Debug + PartialEq + Eq + PartialOrd,
        Value: Clone + Debug + PartialEq,
    {
        match env.msg {
            Set(_id, k, v) => {
                let mut history = history.clone();
                let _ = history.on_invoke(env.src, NolerStoreOp::Set(k.clone(), v.clone()));
                Some(history)
            }

            Get(_id, k) => {
                let mut history = history.clone();
                let _ = history.on_invoke(env.src, NolerStoreOp::Get(k.clone()));
                Some(history)
            }

            _ => None,

        }

    }

    pub fn record_returns<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&KvStoreMsg<RequestId, Key, Value, InternalMsg>>,
    ) -> Option<H>
    where
        H: Clone + ElectionTester<Id, NolerStore<Key, Value>>,
        Key: Clone + Debug + PartialEq + Eq + PartialOrd,
        Value: Clone + Debug + PartialEq,
    {
        match env.msg {
            SetOk(_id, k) => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, NolerStoreRet::SetOk(k.clone()));
                Some(history)
            }
            GetOk(_id, k, v) => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, NolerStoreRet::GetOk(k.clone(), v.clone()));
                Some(history)
            }
            _ => None,
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NolerStoreActor<ServerActor> {
    Client {
        set_count: usize,
        server_count: usize,
    },
    Server(ServerActor),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub enum NolerStoreActorState<ServerState, RequestId> {
    Client {
        awaiting: Option<RequestId>,
        op_count: u64,
    },

    Server(ServerState),
}

impl<ServerActor, InternalMsg> Actor for NolerStoreActor<ServerActor>
where
ServerActor: Actor<Msg = KvStoreMsg<u64, u64, u64, InternalMsg>>,
InternalMsg: Clone + Debug + Eq + Hash

{
    type Msg = KvStoreMsg <u64, u64, u64, InternalMsg>;
    type State = NolerStoreActorState<ServerActor::State, u64>;
    type Timer = ServerActor::Timer;

    fn name(&self) -> String {
        match self {
            NolerStoreActor::Client { .. } => "Client".to_owned(),
            NolerStoreActor::Server(s) => {
                let n = s.name();
                if n.is_empty() {
                    "Server".to_owned()
                } else {
                    n
                }
            }
        }
    }

    #[allow(clippy::identity_op)]
    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        match self {
            NolerStoreActor::Client { 
                set_count,
                server_count,
             } => {
                let server_count = *server_count as u64;

                let index:usize = id.into();

                if (index as u64) < server_count {
                    panic!("Clients must be added to the model after the servers");
                }

                if *set_count == 0 {
                    NolerStoreActorState::Client {
                        awaiting: None,
                        op_count: 0,
                    }
                }

                else {
                    let unique_request_id = 1 * (index as u64);
                    let key = 1 * (index as u64);
                    let value = 1000 + ((index as u64)- server_count);

                    let dst = Id::from((((index as u64) + 0) % server_count) as usize);
                    o.send(
                        dst,
                        Set(unique_request_id, key, value),
                    );

                    NolerStoreActorState::Client {
                        awaiting: Some(unique_request_id),
                        op_count: 1,
                    }
                }
             }
            NolerStoreActor::Server(server_actor) => {
                let mut server_out = Out::new();
                let state = NolerStoreActorState::Server(server_actor.on_start(id, &mut server_out));
                o.append(&mut server_out);
                state
            }
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
            use NolerStoreActor as A;
            use NolerStoreActorState as S;

            match (self, &**state) {
                (A::Client { 
                    set_count,
                    server_count,
                 }, 
                 S::Client { 
                    awaiting: Some(awaiting),
                    op_count,
                 }) => {
                    let server_count = *server_count as u64;

                    match msg {
                        SetOk(request_id, _key) if &request_id == awaiting => {

                            let index:usize = id.into();

                            let unique_request_id = op_count + 1;
                            let key = op_count + 1;

                            if *op_count < *set_count as u64 {
                                let key  = (index as u64) + op_count;
                                let value = 1000 - ((index as u64)- server_count);

                                let dst = Id::from((((index as u64) + op_count) % server_count) as usize);
                                o.send(
                                    dst,
                                    Set(unique_request_id, key, value),
                                );
                            } else {
                                let dst = Id::from((((index as u64) + op_count) % server_count) as usize);
                                o.send(
                                    dst,
                                    Get(unique_request_id, key),
                                );
                            }
                            *state = Cow::Owned(NolerStoreActorState::Client {
                                awaiting: Some(unique_request_id),
                                op_count: *op_count + 1,
                            });
                        }

                        GetOk(request_id, _key, _value) if &request_id == awaiting => {
                                *state = Cow::Owned(NolerStoreActorState::Client {
                                    awaiting: None,
                                    op_count: op_count + 1,
                                });
                        }

                        _ => {}
                    }
                 }


                (A::Server(server_actor), S::Server(server_state)) => {
                    let mut server_state = Cow::Borrowed(server_state);
                    let mut server_out = Out::new();
                    server_actor.on_msg(id, &mut server_state, src, msg, &mut server_out);
                    if let Cow::Owned(server_state) = server_state {
                        *state = Cow::Owned(NolerStoreActorState::Server(server_state))
                    }
                    o.append(&mut server_out);
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
        use NolerStoreActor as A;
        use NolerStoreActorState as S;
        match (self, &**state) {
            (A::Client { .. }, S::Client { .. }) => {}
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                server_actor.on_timeout(id, &mut server_state, timer, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(NolerStoreActorState::Server(server_state))
                }
                o.append(&mut server_out);
            }
            _ => {}
        }
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////

// use ElectionMsg::*;

// impl <Ballot, NolerValue, ConfigSr, InternalMsg> ElectionMsg<Ballot, NolerValue, ConfigSr, InternalMsg> {
//     pub fn record_invocations<C, H>(
//         _cfg: &C,
//         history: &H,
//         env: Envelope<&ElectionMsg<Ballot, NolerValue, ConfigSr, InternalMsg>>,
//     ) -> Option<H>
//     where
//         H: Clone + ElectionTester<Id, Election<Ballot, NolerValue, ConfigSr>>,
//         Ballot: Clone + Debug + PartialEq,
//         NolerValue: Clone + Debug + PartialEq,
//         ConfigSr: Clone + Debug + PartialEq,
//     {
//         // Currently throws away useful information about invalid histories. Ideally
//         // checking would continue, but the property would be labeled with an error.
//         match env.msg {
//             RequestVote(b, v) => {
//                 let mut history = history.clone();
//                 let _ = history.on_invoke(env.src, ElectionOp::RequestVote(b.clone(), v.clone()));
//                 Some(history)
//             }
//             RequestConfig(b) => {
//                 let mut history = history.clone();
//                 let _ = history.on_invoke(env.src, ElectionOp::RequestConfig(b.clone()));
//                 Some(history)
//             }
//             _ => None,
//         }
//     }

//     pub fn record_returns<C, H>(
//         _cfg: &C,
//         history: &H,
//         env: Envelope<&ElectionMsg<Ballot, NolerValue, ConfigSr, InternalMsg>>,
//     ) -> Option<H>
//     where
//         H: Clone + ElectionTester<Id, Election<Ballot, NolerValue, ConfigSr>>,
//         Ballot: Clone + Debug + PartialEq,
//         NolerValue: Clone + Debug + PartialEq,
//         ConfigSr: Clone + Debug + PartialEq,
//     {
//         // Currently throws away useful information about invalid histories. Ideally
//         // checking would continue, but the property would be labeled with an error.
//         match env.msg {
//             ResponseVote(b) => {
//                 let mut history = history.clone();
//                 let _ = history.on_return(env.dst, ElectionRet::ResponseVote(b.clone()));
//                 Some(history)
//             }
//             Config(b, c) => {
//                 let mut history = history.clone();
//                 let _ = history.on_return(env.dst, ElectionRet::Config(b.clone(), c.clone()));
//                 Some(history)
//             }
//             _ => None,
//         }
//     }
// }


// #[derive(Clone, Debug, Eq, PartialEq)]
// pub enum ElectionActor<ServerActor> {
//     /// A client that [`ElectionMsg::RequestVote`]s a message and upon receving a
//     /// corresponding [`ElectionMsg::ResponseVote`] follows up with a
//     /// [`ElectionMsg::RequestConfig`].
//     // Client {
//     //     request_vote_count: usize,
//     //     server_count: usize,
//     // },
//     /// A server actor being validated.
//     Server(ServerActor),
// }

// #[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
// pub enum ElectionActorState<ServerState> {
//     Server(ServerState),
// }

// impl<ServerActor, InternalMsg> Actor for ElectionActor<ServerActor>
// where
//     ServerActor: Actor<Msg = ElectionMsg<(u32, u64), (Role, ElectionType, u8), ConfigSr, InternalMsg>>, //ToDO
//     InternalMsg: Clone + Debug + Eq + Hash,
// {
//     type Msg = ElectionMsg<(u32, u64), (Role, ElectionType, u8), ConfigSr, InternalMsg>;
//     type State = ElectionActorState<ServerActor::State>;
//     type Timer = ServerActor::Timer;

//     fn name(&self) -> String {
//         match self {
//             //ElectionActor::Client { .. } => "Client".to_owned(),
//             ElectionActor::Server(s) => {
//                 let n = s.name();
//                 if n.is_empty() {
//                     "Server".to_owned()
//                 } else {
//                     n
//                 }
//             }
//         }
//     }

//     #[allow(clippy::identity_op)]
//     fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
//         match self {
//             ElectionActor::Server(server_actor) => {
//                 let mut server_out = Out::new();
//                 let state = ElectionActorState::Server(server_actor.on_start(id, &mut server_out));
//                 o.append(&mut server_out);
//                 state
//             }
//         }
//     }

//     fn on_msg(
//         &self,
//         id: Id,
//         state: &mut Cow<Self::State>,
//         src: Id,
//         msg: Self::Msg,
//         o: &mut
//         Out<Self>,
//     ) {
//         use ElectionActor as A;
//         use ElectionActorState as S;

//         match (self, &**state) {
//             (A::Server(server_actor), S::Server(server_state)) => {
//                 let mut server_state = Cow::Borrowed(server_state);
//                 let mut server_out = Out::new();

//                 match msg {
//                     // ElectionMsg::RequestVote(ballot, _value) => {

//                     // }
//                     // ElectionMsg::ResponseVote(ballot) => {

//                     // }

//                     // ElectionMsg::Config(ballot, config) => {

//                     // }

//                     // ElectionMsg::RequestConfig(ballot) => {

//                     // }

//                     _ => {}
//                 }


//                 server_actor.on_msg(id, &mut server_state, src, msg, &mut server_out);
//                 if let Cow::Owned(server_state) = server_state {
//                     *state = Cow::Owned(ElectionActorState::Server(server_state))
//                 }
//                 o.append(&mut server_out);
//             }
//             //_ => {}
//         }
//     }

//     fn on_timeout(
//         &self,
//         id: Id,
//         state: &mut Cow<Self::State>,
//         timer: &Self::Timer,
//         o: &mut Out<Self>,
//     ) {
//         use ElectionActor as A;
//         use ElectionActorState as S;
//         match (self, &**state) {
//            // (A::Client { .. }, S::Client { .. }) => {}
//             (A::Server(server_actor), S::Server(server_state)) => {
//                 let mut server_state = Cow::Borrowed(server_state);
//                 let mut server_out = Out::new();
//                 server_actor.on_timeout(id, &mut server_state, timer, &mut server_out);
//                 if let Cow::Owned(server_state) = server_state {
//                     *state = Cow::Owned(ElectionActorState::Server(server_state))
//                 }
//                 o.append(&mut server_out);
//             }
//             //_ => {}
//         }
//     }

// }

////////////////////////////////////////////////////////////////////////////////////////////////////
use NolerElectionMsg::*;

impl <Ballot, ConfigSR, InternalMsg> NolerElectionMsg<Ballot, ConfigSR, InternalMsg> {
    pub fn record_invocations<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&NolerElectionMsg<Ballot, ConfigSR, InternalMsg>>,
    ) -> Option<H>
    where
        H: Clone + ElectionTester<Id, NolerElection<Ballot, Id, ConfigSR>>,
        Ballot: Clone + Debug + PartialEq + Eq + PartialOrd,
        Role: Clone + Debug + PartialEq,
        ConfigSR: Clone + Debug + PartialEq,
        ElectionType: Clone + Debug + PartialEq,
    {
        match env.msg {
            NolerRequestVote(b, nv) => {

                let (r, e, p) = nv.clone();


                let mut history = history.clone();
                let _ = history.on_invoke(env.src, NolerElectionOp::RequestVote(b.clone(), (r, e, p)));
                Some(history)
            }
            NolerRequestConfig(b) => {
                let mut history = history.clone();
                let _ = history.on_invoke(env.src, NolerElectionOp::RequestConfig(b.clone()));
                Some(history)
            }
            _ => None,
        }
    }

    pub fn record_returns<C, H>(
        _cfg: &C,
        history: &H,
        env: Envelope<&NolerElectionMsg<Ballot, ConfigSR, InternalMsg>>,
    ) -> Option<H>
    where
        H: Clone + ElectionTester<Id, NolerElection<Ballot, Id, ConfigSr>>,
        Id: Clone + Debug + PartialEq + Copy,
        Ballot: Clone + Debug + PartialEq + Eq + PartialOrd,
        Role: Clone + Debug + PartialEq,
        ConfigSr: Clone + Debug + PartialEq,
        ElectionType: Clone + Debug + PartialEq,
    {
        match env.msg {
            NolerResponseVote(b) => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, NolerElectionRet::ResponseVote(b.clone()));
                Some(history)
            }
            NoVote => {
                let mut history = history.clone();
                let _ = history.on_return(env.dst, NolerElectionRet::NoVote);
                Some(history)
            }
            NolerConfig(b, c) => {
                let mut history = history.clone();
                let config = c.clone();
                //let _ = history.on_return(env.dst, NolerElectionRet::Config(*config));
                Some(history)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NolerElectionActor<ServerActor> {
    Server(ServerActor),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub enum NolerElectionActorState<ServerState> {
    Server(ServerState),
}

impl<ServerActor, InternalMsg> Actor for NolerElectionActor<ServerActor>
where
    ServerActor: Actor<Msg = NolerElectionMsg<(u32, u64), ConfigSr, InternalMsg>>,
    InternalMsg: Clone + Debug + Eq + Hash,
{
    type Msg = NolerElectionMsg<(u32, u64), ConfigSr, InternalMsg>;
    type State = NolerElectionActorState<ServerActor::State>;
    type Timer = ServerActor::Timer;

    fn name(&self) -> String {
        match self {
            NolerElectionActor::Server(s) => {
                let n = s.name();
                if n.is_empty() {
                    "Server".to_owned()
                } else {
                    n
                }
            }
        }
    }

    #[allow(clippy::identity_op)]
    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        match self {
            NolerElectionActor::Server(server_actor) => {
                let mut server_out = Out::new();
                let state = NolerElectionActorState::Server(server_actor.on_start(id, &mut server_out));
                o.append(&mut server_out);
                state
            }
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut
        Out<Self>,
    ) {
        use NolerElectionActor as A;
        use NolerElectionActorState as S;

        match (self, &**state) {
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();

                server_actor.on_msg(id, &mut server_state, src, msg, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(NolerElectionActorState::Server(server_state))
                }
                o.append(&mut server_out);                
            }
        }
    }

    fn on_timeout(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        use NolerElectionActor as A;
        use NolerElectionActorState as S;
        match (self, &**state) {
            (A::Server(server_actor), S::Server(server_state)) => {
                let mut server_state = Cow::Borrowed(server_state);
                let mut server_out = Out::new();
                server_actor.on_timeout(id, &mut server_state, timer, &mut server_out);
                if let Cow::Owned(server_state) = server_state {
                    *state = Cow::Owned(NolerElectionActorState::Server(server_state))
                }
                o.append(&mut server_out);
            }
        }
    }

}