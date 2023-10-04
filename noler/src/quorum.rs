use std::{collections::HashMap, net::SocketAddr};

type Ballot = (u32, u64);

#[derive(Debug, PartialEq, Clone)]
pub struct QuorumSet<IDTYPE: std::cmp::Eq + std::hash::Hash, MSGTYPE> {
    num_required: i32,
    messages: HashMap<IDTYPE, HashMap<SocketAddr, MSGTYPE>>,
}

impl<IDTYPE, MSGTYPE> QuorumSet<IDTYPE, MSGTYPE>
where
    IDTYPE: Eq + std::hash::Hash + Clone,
{
    pub fn new(num_required: i32) -> Self {
        QuorumSet {
            num_required,
            messages: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn clear_id(&mut self, vs: IDTYPE) {
        self.messages.entry(vs).or_insert(HashMap::new()).clear();
    }

    pub fn num_required(&self) -> i32 {
        self.num_required
    }

    pub fn get_messages(&self, vs: IDTYPE) -> Option<&HashMap<SocketAddr, MSGTYPE>> {
        self.messages.get(&vs)
    }

    pub fn check_for_quorum(&self, vs: IDTYPE) -> Option<&HashMap<SocketAddr, MSGTYPE>> {
        if let Some(vsmessages) = self.messages.get(&vs) {
            if vsmessages.len() >= self.num_required as usize {
                return Some(vsmessages);
            }
        }
        None
    }

    pub fn add_and_check_for_quorum(
        &mut self,
        vs: IDTYPE,
        replica_idx: SocketAddr,
        msg: MSGTYPE,
    ) -> Option<&HashMap<SocketAddr, MSGTYPE>> {
        let vsmessages = self.messages.entry(vs.clone()).or_insert(HashMap::new());

        vsmessages.insert(replica_idx, msg);

        self.check_for_quorum(vs)
    }

    pub fn add(&mut self, vs: IDTYPE, replica_idx: SocketAddr, msg: MSGTYPE) {
        self.add_and_check_for_quorum(vs, replica_idx, msg);
    }
}