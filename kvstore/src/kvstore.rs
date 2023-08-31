use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]// Define the KeyValueStore struct
pub struct KVStore {
    data: Arc<RwLock<HashMap<String, String>>>,
}

impl KVStore {
    // Implement methods to interact with the store (set, get, update, delete, etc.)
    pub fn new() -> Self {
        KVStore {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.read().unwrap();

        if data.contains_key(key) {
            println!("Key found in GET - {}", key);
            data.get(key).cloned()
        } else {
            println!("Key not found in GET");
            None
        }
    }

    pub fn set(&self, key: String, value: String) {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_string(), value.to_string());

    }

    pub fn delete(&self, key: &str)  {
        let mut data = self.data.write().unwrap();
        if data.contains_key(key) {
            data.remove(key);
        } else {
            println!("Key not found in DELETE");
        }
    }

    pub fn append(&self, key: &str, value: String) {
        let mut data = self.data.write().unwrap();

        if data.contains_key(key) {
            let mut value = value;
            value.push_str(&data.get(key).unwrap());
            data.insert(key.to_string(), value.to_string());
        } else {
            println!("Key not found in APPEND");
        }
    }

    pub fn get_data(&self) -> HashMap<String, String> {
        let data = self.data.read().unwrap();
        data.clone()
    }
    
}