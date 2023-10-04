use stateright::semantics::SequentialSpec;
use std::fmt::Debug;
use noler::{role::Role, constants::Status};
use noler::message::ElectionType;

//////////////////////////////////////////////////////////////////////////////////////////
#[derive(Clone, Default, Debug, Hash, PartialEq, serde::Serialize)]
pub struct Noler<T, V>(
    pub T, //Ballot
    pub Option<V>, //ConfigSr
);

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum NolerOp<T> {
    Vote(T),
    //VoteResponse(T),
    //ConfigRequest(T),
}

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum NolerRet<T, V> {
    VoteOk(T),
    NoVote(T),
    Configuration(T, V),
    //ConfigOk(T, U, V),
}

impl <T: Clone + Debug + PartialEq, V: Clone + Debug + PartialEq> SequentialSpec for Noler<T, V> {
    type Op = NolerOp<T>;
    type Ret = NolerRet<T, V>;
    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            NolerOp::Vote(b) => {
                self.0 = b.clone();
                NolerRet::VoteOk(b.clone())
            }
        }
    }
    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        // Override to avoid unnecessary `clone` on `Read`.
        match (op, ret) {
            (NolerOp::Vote(b), NolerRet::VoteOk(c)) => {
                if b == c {
                    self.0 = b.clone();
                    true
                }
                else {
                    false
                }
            }
            (NolerOp::Vote(b), NolerRet::NoVote(c)) => {
                if b == c {
                    true
                }
                else {
                    false
                }
            }
            (_, NolerRet::Configuration(c, d)) => {
                if self.0 == *c {
                    self.1 = Some(d.clone());
                    true
                }
                else {
                    false
                }
            }

            //_ => false,
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub struct NolerStore <K, V> (
    pub K,
    pub V,
);

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum NolerStoreOp <K, V> {
    Get(K),
    Set(K, V),
}

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum NolerStoreRet <K, V> {
    GetOk(K, V),
    SetOk(K),
}

impl <K: Clone + Debug + PartialEq, V: Clone + Debug + PartialEq> SequentialSpec for NolerStore<K, V> {
    type Op = NolerStoreOp<K, V>;
    type Ret = NolerStoreRet<K, V>;
    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            NolerStoreOp::Get(k) => {
                NolerStoreRet::GetOk(k.clone(), self.1.clone())
            }
            NolerStoreOp::Set(k, v) => {
                self.0 = k.clone();
                self.1 = v.clone();

                NolerStoreRet::SetOk(k.clone())
            }
        }
    }
    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        // Override to avoid unnecessary `clone` on `Read`.
        match (op, ret) {
            (NolerStoreOp::Get(k), NolerStoreRet::GetOk(c, _d)) => {
                if k == c {
                    true
                }
                else {
                    false
                }
            }
            (NolerStoreOp::Set(k, _v), NolerStoreRet::SetOk(c)) => {
                if k == c {
                    true
                }
                else {
                    false
                }
            }
            _ => false,
        }
    }
}
//////////////////////////////////////////////////////////////////////////////////////////













#[derive(Clone, Default, Debug, Hash, PartialEq, serde::Serialize)]
pub struct Election<T, U, V>(
    pub T,
    pub U,
    pub V,
);

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum ElectionOp<T, U> {
    RequestVote(T, U),
    RequestConfig(T),
}

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum ElectionRet<T, V> {
    ResponseVote(T),
    Config(T, V),
}

impl <T: Clone + Debug + PartialEq, U: Clone + Debug + PartialEq, V: Clone + Debug + PartialEq> SequentialSpec for Election<T, U, V> {
    type Op = ElectionOp<T, U>;
    type Ret = ElectionRet<T, V>;
    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            ElectionOp::RequestVote(b, v) => {
                ElectionRet::ResponseVote(b.clone())
            }
            ElectionOp::RequestConfig(b) => {
                ElectionRet::Config(b.clone(), self.2.clone())
            }
        }
    }
    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        // Override to avoid unnecessary `clone` on `Read`.
        match (op, ret) {
            (ElectionOp::RequestVote(b, v), ElectionRet::ResponseVote(c)) => b == c,
            (ElectionOp::RequestConfig(b), ElectionRet::Config(c, d)) => b == c,
            _ => false,
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
//Ballot, NolerValue, ConfigSr
//NolerValue -> (Role, ElectionType, u8)
#[derive(Clone, Default, Debug, Hash, PartialEq, serde::Serialize)]
pub struct NolerElection<T, I, V>( //T - Ballot, V - ConfigSr
    pub T, //Ballot
    pub Option<(I, T)>, //Leader
    pub Option<V>, //ConfigSr
);


//RequestVote(ballot, (_replica_role, election_type, profile)
#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum NolerElectionOp<T, X, Y, Z> {
    RequestVote(T, (X, Y, Z)),
    RequestConfig(T),
}

#[derive(Clone, Debug, Hash, PartialEq, serde::Serialize)]
pub enum NolerElectionRet<T, V> {
    ResponseVote(T),
    Config(V),
    NoVote,
}

impl<T, I, V> SequentialSpec for NolerElection<T, I, V>
where
    T: Clone + Debug + PartialEq + PartialOrd,
    I: Clone + Debug + PartialEq,
    //X: Clone + Debug + PartialEq,
    V: Clone + Debug + PartialEq,
    //Y: Clone + Debug + PartialEq,
    //Z: Clone + Debug + PartialEq,
{
    type Op = NolerElectionOp<T, Role, ElectionType, u8>;
    type Ret = NolerElectionRet<T, V>;
    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            NolerElectionOp::RequestVote(b, v) => {
                //T - Ballot, X - Role, Y - ElectionType, Z - u8, V - ConfigSr
                //N - (Role, ElectionType, u8)

                let (role, election_type, p_l) = v.clone();

                let p_r = 80;

                //////////////////////////////////////////////////////////////////////////
                
                match election_type {
                    ElectionType::Normal => {

                        if role == Role::Member 
                        {
                            log::info!("Normal Election Path");

                            if *b > self.0 {
                                if p_r >= p_l {
                                    NolerElectionRet::ResponseVote(b.clone())
                                }
                                else {
                                    NolerElectionRet::NoVote
                                }
                            }
                            else {
                                NolerElectionRet::NoVote
                            }
                        }
                        else {
                            NolerElectionRet::NoVote
                        }
                    }



                    ElectionType::Degraded => {

                        if role == Role::Member {
                            if *b >= self.0 {
                                NolerElectionRet::ResponseVote(b.clone())
                            }
                            else {
                                NolerElectionRet::NoVote
                            }
                        }
                        else {
                            NolerElectionRet::NoVote
                        }
                    }

                    ElectionType::Offline => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                NolerElectionRet::ResponseVote(b.clone())
                            }
                            else {
                                NolerElectionRet::NoVote
                            }
                        }
                        else {
                            NolerElectionRet::NoVote
                        }
                    }

                    ElectionType::Timeout  => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                NolerElectionRet::ResponseVote(b.clone())
                            }
                            else {
                                NolerElectionRet::NoVote
                            }
                        }
                        else {
                            NolerElectionRet::NoVote
                        }

                    }

                    ElectionType::Profile => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                NolerElectionRet::ResponseVote(b.clone())
                            }
                            else {
                                NolerElectionRet::NoVote
                            }
                        }
                        else {
                            NolerElectionRet::NoVote
                        }
                    }
                }

            }
            _=> {NolerElectionRet::NoVote}
        }
    }
    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {

        match (op, ret) {

            (NolerElectionOp::RequestVote(b, (role, election_type, p_l)), NolerElectionRet::ResponseVote(c)) => {

                let role = role.clone();
                let election_type = election_type.clone();
                let p_l = p_l.clone();

                let p_r = 80;

                match election_type {

                    ElectionType::Normal => {
                        if role == Role::Member {
                            log::info!("Normal Election Path");

                            if *b > self.0 {
                                if p_r >= p_l {
                                    b == c
                                }
                                else {
                                    false
                                }
                            }
                            else {
                                false
                            }
                        }
                        else {
                            false
                        }
                    }

                    ElectionType::Degraded => {
                        if role == Role::Member {
                            if *b >= self.0 {
                                b == c
                            }
                            else {
                                false
                            }
                        }
                        else {
                            false
                        }
                    }

                    ElectionType::Offline =>  {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                b == c
                            }
                            else {
                                false
                            }
                        }
                        else {
                            false
                        }
                    }


                    ElectionType::Timeout  => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                b == c
                            }
                            else {
                                false
                            }
                        }
                        else {
                            false
                        }
                    }

                    ElectionType::Profile => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                b == c
                            }
                            else {
                                false
                            }
                        }
                        else {
                            false
                        }
                    }
                }
            }


            (NolerElectionOp::RequestVote(b, (role, election_type, p_l)), NolerElectionRet::NoVote) => {
                let role = role.clone();
                let election_type = election_type.clone();
                let p_l = p_l.clone();

                let p_r = 80;

                match election_type {

                    ElectionType::Normal => {
                        if role == Role::Member {
                            log::info!("Normal Election Path");

                            if *b > self.0 {
                                if p_r >= p_l {
                                    false
                                }
                                else {
                                    true
                                }
                            }
                            else {
                                true
                            }
                        }
                        else {
                            true
                        }
                    }

                    ElectionType::Degraded => {
                        if role == Role::Member {
                            if *b >= self.0 {
                                false
                            }
                            else {
                                true
                            }
                        }
                        else {
                            true
                        }
                    }

                    ElectionType::Offline => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                false
                            }
                            else {
                                true
                            }
                        }
                        else {
                            true
                        }
                    }

                    ElectionType::Timeout => {
                        if role == Role::Candidate 
                        {
                            if *b > self.0 {
                                false
                            }
                            else {
                                true
                            }
                        }
                        else {
                            true
                        }
                    }

                    ElectionType::Profile => {
                        if role == Role::Candidate {
                            if *b > self.0 {
                                false
                            }
                            else {
                                true
                            }
                        }
                        else {
                            true
                        }
                    }
                }
            }

            _ => {false}
        }
    }
}

//////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
#[rustfmt::skip]
mod test {
    use super::*;
    use noler::config::ConfigSr;
    use noler::message::ElectionType;
    use noler::role::Role;
    use stateright::actor::Id;

    #[test]
    fn models_nolerelection_semantics() {
        let mut r1: NolerElection<(i32, i32), Id, ConfigSr> = NolerElection((0, 0), None, None);

        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Normal, 50))), NolerElectionRet::ResponseVote((1, 1)), "Normal Election Path Pass"); // < 80
        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Timeout, 0))), NolerElectionRet::NoVote, "Timeout Election Path - NoVote");
        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Degraded, 0))), NolerElectionRet::ResponseVote((1, 1)), "Degraded Election Path Pass"); //Should pass with a ResponseVote
        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((1, 1), (Role::Candidate, ElectionType::Profile, 0))), NolerElectionRet::ResponseVote((1, 1)), "Profile Election Path - NoVote");
        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Offline, 0))), NolerElectionRet::NoVote, "Offline Election Path - NoVote");
        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((0, 0), (Role::Member, ElectionType::Normal, 0))), NolerElectionRet::NoVote); //Should pass with a NoVote
        assert_eq!(r1.invoke(&NolerElectionOp::RequestVote((1, 1), (Role::Candidate, ElectionType::Timeout, 0))), NolerElectionRet::ResponseVote((1, 1))); //Should pass with a ResponseVote

    }

    #[test]
    fn models_nolerelection_valid_histories() {

        let mut r1: NolerElection<(i32, i32), Id, ConfigSr> = NolerElection((0, 0), None, None);

        assert!(r1.is_valid_history(vec![])); //Pass
        
        assert!(r1.is_valid_history(vec![
            (NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Normal, 0)), NolerElectionRet::ResponseVote((1, 1))),
            (NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Normal, 0)), NolerElectionRet::ResponseVote((1, 1))), //Should not be valid as the ballot is not greater than the previous one
            (NolerElectionOp::RequestVote((2, 2), (Role::Candidate, ElectionType::Timeout, 0)), NolerElectionRet::ResponseVote((2, 2))),
        ]));
    }

    #[test]
    fn models_nolerelection_invalid_histories() {

        let mut r1: NolerElection<(i32, i32), Id, ConfigSr> = NolerElection((0, 0), None, None);

        assert!(!r1.is_valid_history(vec![
            (NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Normal, 0)), NolerElectionRet::ResponseVote((1, 1))),
            (NolerElectionOp::RequestVote((2, 2), (Role::Candidate, ElectionType::Offline, 0)), NolerElectionRet::ResponseVote((2, 2))),
            (NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Degraded, 0)), NolerElectionRet::ResponseVote((1, 1))),
        ]));

        assert!(!r1.is_valid_history(vec![
            (NolerElectionOp::RequestVote((1, 1), (Role::Member, ElectionType::Normal, 0)), NolerElectionRet::ResponseVote((1, 1))),
            (NolerElectionOp::RequestVote((1, 0), (Role::Member, ElectionType::Normal, 0)), NolerElectionRet::ResponseVote((1, 1))),
        ]));
    }

    #[test]
    fn models_noler_semantics() {
        let mut r1: Noler<(i32, i32), ConfigSr> = Noler((0, 0), None);

        assert_eq!(r1.invoke(&NolerOp::Vote((1, 1))), NolerRet::VoteOk((1, 1)));
        assert_eq!(r1.invoke(&NolerOp::Vote((1, 1))), NolerRet::NoVote((0, 0)));
        assert_eq!(r1.invoke(&NolerOp::Vote((1, 1))), NolerRet::VoteOk((1, 1)));
    }

    #[test]
    fn models_nolerstore_semantics() {
        let mut kv = NolerStore(0, 0);

        assert_eq!(kv.invoke(&NolerStoreOp::Get(0)), NolerStoreRet::GetOk(0, 0));
        assert_eq!(kv.invoke(&NolerStoreOp::Set(1, 1)), NolerStoreRet::SetOk(1));
        assert_eq!(kv.invoke(&NolerStoreOp::Get(1)), NolerStoreRet::GetOk(1, 1));
        assert_eq!(kv.invoke(&NolerStoreOp::Set(1, 2)), NolerStoreRet::SetOk(1));
        assert_eq!(kv.invoke(&NolerStoreOp::Get(1)), NolerStoreRet::GetOk(1, 2));
        //assert_eq!(kv.invoke(&NolerStoreOp::Get(0)), NolerStoreRet::GetOk(0, 0)); //Fail

    }

}