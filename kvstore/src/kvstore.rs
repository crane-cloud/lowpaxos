// use std::fmt::Result;
use serde::{Serialize, Deserialize};
use stateright::util::HashableHashMap;
// use stateright::actor::register::RegisterMsg;
// use noler::noler_msg_checker::NolerMsg;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]// Define the KeyValueStore struct
pub struct KVStore {
    data: HashableHashMap<String, String>,
}

impl KVStore{
    pub fn new() -> Self {
        KVStore {
            data: HashableHashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if self.data.contains_key(key) {
            println!("Key found in GET - {}", key);
            self.data.get(key).cloned()
        } else {
            println!("Key not found in GET");
            None
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key.to_string(), value.to_string());
    }

    pub fn delete(&mut self, key: &str)  {
        if self.data.contains_key(key) {
            self.data.remove(key);
        } else {
            println!("Key not found in DELETE");
        }
    }

    pub fn append(&mut self, key: &str, value: String) {
        if self.data.contains_key(key) {
            let mut value = value;
            value.push_str(&self.data.get(key).unwrap());
            self.data.insert(key.to_string(), value.to_string());
        } else {
            println!("Key not found in APPEND");
        }
    }

    pub fn get_data(&self) -> HashableHashMap<String, String> {
        self.data.clone()
    }
}


// type RequestId = u64;
// type Value = char;

// #[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
// pub enum ReplyMessage {
//     PutOk(RequestId),
//     GetOk(RequestId, Value),
// }


// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
// pub struct KVStoreSR {
//     data: HashableHashMap<u64, char>,
// }

// impl KVStoreSR {
//     pub fn new() -> Self {
//         KVStoreSR {
//             data: HashableHashMap::new(),
//         }
//     }

//     pub fn put(&mut self, key: u64, value: char) -> RegisterMsg<RequestId, Value, NolerMsg> {
//         self.data.insert(key, value);
//         RegisterMsg::PutOk(key)
//     }

//     pub fn get(&self, key: u64) -> RegisterMsg<RequestId, Value, NolerMsg> {
//         if self.data.contains_key(&key) {
//             RegisterMsg::GetOk(key, *self.data.get(&key).unwrap())
//         } else {
//             RegisterMsg::GetOk(key, ' ')
//         }
//     }
// }

// pub struct KVStore {
//     data: Arc<RwLock<HashMap<String, String>>>,
// }

// impl KVStore {
//     // Implement methods to interact with the store (set, get, update, delete, etc.)
//     pub fn new() -> Self {
//         KVStore {
//             data: Arc::new(RwLock::new(HashMap::new())),
//         }
//     }
//     pub fn get(&self, key: &str) -> Option<String> {
//         let data = self.data.read().unwrap();

//         if data.contains_key(key) {
//             println!("Key found in GET - {}", key);
//             data.get(key).cloned()
//         } else {
//             println!("Key not found in GET");
//             None
//         }
//     }

//     pub fn set(&self, key: String, value: String) {
//         let mut data = self.data.write().unwrap();
//         data.insert(key.to_string(), value.to_string());

//     }

//     pub fn delete(&self, key: &str)  {
//         let mut data = self.data.write().unwrap();
//         if data.contains_key(key) {
//             data.remove(key);
//         } else {
//             println!("Key not found in DELETE");
//         }
//     }

//     pub fn append(&self, key: &str, value: String) {
//         let mut data = self.data.write().unwrap();

//         if data.contains_key(key) {
//             let mut value = value;
//             value.push_str(&data.get(key).unwrap());
//             data.insert(key.to_string(), value.to_string());
//         } else {
//             println!("Key not found in APPEND");
//         }
//     }

//     pub fn get_data(&self) -> HashMap<String, String> {
//         let data = self.data.read().unwrap();
//         data.clone()
//     }
    
// }