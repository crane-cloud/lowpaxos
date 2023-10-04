use stateright::util::HashableHashMap;
use crate::noler_msg_checker::NolerMsg;
use crate::election_actor::KvStoreMsg;

type RequestId = u64;
type Key = u64;
type Value = u64;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KVStoreSR {
    data: HashableHashMap<u64, u64>,
}

impl KVStoreSR {
    pub fn new() -> Self {
        KVStoreSR {
            data: HashableHashMap::new(),
        }
    }

    pub fn set(&mut self, request_id: u64, key: u64, value: u64) -> KvStoreMsg<RequestId, Key, Value, NolerMsg> {
        self.data.insert(key, value);
        KvStoreMsg::SetOk(request_id, key)
    }

    pub fn get(&self, request_id: u64, key: u64) -> KvStoreMsg<RequestId, Key, Value, NolerMsg> {
        if self.data.contains_key(&key) {
            KvStoreMsg::GetOk(request_id, key, *self.data.get(&key).unwrap())
        } else {
            KvStoreMsg::GetOk(request_id, key, 0)
        }
    }
}