pub mod election_tester;
pub mod leaderelection_tester;

pub mod kvstoresr;
pub mod logsr;

pub mod noler_msg_checker {

    use serde::{Deserialize, Serialize};
    use stateright::actor::Id;

    use noler::message::ElectionType;
    use noler::role::Role;
    use noler::config::ConfigSr;

    type Ballot = (u32, u64);
    type RequestId = u64;
    type Value = char;
    type Request = (RequestId, Id, Option<Value>);
    // type ValueNoler = (u64, Option<String>); //KV Operation (key, option<String Value>)
    // type RequestNoler = (RequestId, Id, ValueNoler);

    #[allow(dead_code)]
    #[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, Hash, PartialEq)]
    pub struct RequestVoteMessage {
        pub id: Id,
        pub replica_role: Role,
        pub ballot: (u32, u64),
        pub election_type: ElectionType,
        pub profile: u8,
    }

    #[allow(dead_code)]
    #[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, Hash, PartialEq)]
    pub struct ResponseVoteMessage {
        pub id: Id,
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
        pub leader: Id,
        pub config: ConfigSr,
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

    #[allow(dead_code)]
    #[derive(Debug, Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
    pub struct ReplyMessage {
        
    }

    #[derive(Clone, Debug, Deserialize, Serialize, Eq, Hash, PartialEq)]
    pub enum NolerMsg {
        RequestVote(RequestVoteMessage),
        ResponseVote(ResponseVoteMessage),
        Config(ConfigMessage),
        HeartBeat(HeartBeatMessage),
        RequestConfig(RequestConfigMessage),
        PollLeader(PollLeaderMessage),
        PollLeaderOk(PollLeaderOkMessage),

        //Paxos-related messages

        PutInternal {
           src: Id,
           request_id: RequestId,
           value: Value,
        },

        GetInternal {
            id: Id,
            src: Id,
            request_id: RequestId,
            //value: Value,
        },
        
        Propose {
            id: Id,
            ballot: Ballot,
            request: Request,
        },

        ProposeOk {
            id: Id,
            ballot: Ballot,
            commit_index: u64,
            request: Request,
        },

        Commit {
            id: Id,
            ballot: Ballot,
            request: Request,
        },

        //SR related messages
        Prepare {
            ballot: Ballot,
        },

        Prepared {
            ballot: Ballot,
            last_accepted: Option<(Ballot, (RequestId, u32, Value))>,
        },

        // //KV Store related messages
        // Put {
        //     request_id: RequestId,
        //     value: Value,
        //     //value: ValueNoler,
        // },

        // Get {
        //     request_id: RequestId,
        //     //value: ValueNoler,
        // },
    }


    #[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
    pub enum ElectionTimer {
        LeaderInitTimeout,
        LeadershipVoteTimeout,
        LeaderVoteTimeout,
        // LeaderLeaseTimeout,
        // HeartBeatTimeout,
        // PollLeaderTimeout,
        // PollLeaderTimer,
    }

}