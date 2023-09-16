use stateright::util::HashableHashMap;
use stateright::actor::register::RegisterMsg;
use crate::noler_msg_checker::NolerMsg;

type RequestId = u64;
type Value = char;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct KVStoreSR {
    data: HashableHashMap<u64, char>,
}

impl KVStoreSR {
    pub fn new() -> Self {
        KVStoreSR {
            data: HashableHashMap::new(),
        }
    }

    pub fn put(&mut self, key: u64, value: char) -> RegisterMsg<RequestId, Value, NolerMsg> {
        self.data.insert(key, value);
        RegisterMsg::PutOk(key)
    }

    pub fn get(&self, key: u64) -> RegisterMsg<RequestId, Value, NolerMsg> {
        if self.data.contains_key(&key) {
            RegisterMsg::GetOk(key, *self.data.get(&key).unwrap())
        } else {
            RegisterMsg::GetOk(key, '?')
        }
    }
}