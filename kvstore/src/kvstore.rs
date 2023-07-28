use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// Define the KeyValueStore struct
pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
    //log: Arc<RwLock<Vec<String>>>,
    //commit_log: Arc<RwLock<Vec<String>>>,
}

impl KeyValueStore {
    // Implement methods to interact with the store (set, get, update, delete, etc.)
    pub fn new() -> Self {
        KeyValueStore {
            data: Arc::new(RwLock::new(HashMap::new())),
            //log: Arc::new(RwLock::new(Vec::new())),
            //commit_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn set(&self, key: String, value: String) {
        let mut data = self.data.write().unwrap();
        data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.read().unwrap();
        data.get(key).cloned()
    }

    pub fn delete(&self, key: &str) -> Option<String> {
        let mut data = self.data.write().unwrap();
        data.remove(key)
    }

    pub fn append(&self, key: &str, value: String) -> bool {
        let mut data = self.data.write().unwrap();
        if let Some(current_value) = data.get_mut(key) {
            current_value.push_str(&value);
            true
        } else {
            false
        }
    }   
}