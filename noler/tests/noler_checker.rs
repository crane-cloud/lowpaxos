// use noler::role::Role;
// use noler::message::ElectionType;
// use noler::message::{
//     RequestVoteMessage,
//     ResponseVoteMessage,
//     ConfigMessage,
// };
// use noler::constants;
// use noler::quorum::QuorumSet;

// use serde::{Deserialize, Serialize};
// use serde_json::Value;
// use stateright::actor::register::{RegisterActor, RegisterMsg, RegisterMsg::*};
// use stateright::actor::{majority, model_peers, Actor, ActorModel, Id, Network, Out};
// use stateright::report::WriteReporter;
// use stateright::semantics::register::Register;
// use stateright::semantics::LinearizabilityTester;
// use stateright::util::{HashableHashMap, HashableHashSet};
// use stateright::{Checker, Expectation, Model};
// use std::borrow::Cow;
// use std::net::SocketAddr;

// // type ProposeTerm = u32;
// // type ElectionType = u32;


// #[derive(Clone, Debug, Deserialize, Serialize)]
// enum NolerMsg {
//     RequestVote(RequestVoteMessage),
//     ResponseVote(ResponseVoteMessage),
//     Config(ConfigMessage),
// }


// use NolerMsg::*;

// #[derive(Clone, Debug)]
// struct NolerState {
//     id: u32,
//     replica_address: SocketAddr,
//     role: Role,
//     state: u8,
//     propose_term: u32,
//     propose_number: u64,
//     voted: (u32, Option<u32>),
//     leader: Option<u32>, //To be part of the config
//     leadership_quorum: QuorumSet<(u32, u32), ResponseVoteMessage>,
// }

// #[derive(Clone, Debug)]
// struct NolerActor {
//     peer_ids: Vec<Id>,
// }


// impl Actor for NolerActor {

// }

// #[derive(Clone, Debug)]
// struct NolerModelCfg {
//     peer_count: usize,
//     network: Network<<NolerActor as Actor>::Msg>,
// }

// impl NolerModelCfg {
//     fn into_model(
//         self,
//     ) -> ActorModel<RegisterActor<NolerActor>, Self, LinearizabilityTester<Id, _>> {
//         ActorModel::new(
//             self.clone(),
//             LinearizabilityTester::new(
//                 Register(Value::Null)),
//         )
//     }
// }




// #[cfg(test)]
// #[test]
// fn can_model_noler(){
//     use stateright::actor::ActorModelAction::Deliver;

//     let checker = NolerModelCfg {
//         peer_count: 5,
//         network: Network::new_unordered_nonduplicating([]),
//     }
//     .into_model()
//     .checker()
//     .spawn_bfs()
//     .join();

//     checker.assert_properties();
// }


// fn main() {

// }