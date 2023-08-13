use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// Define the KeyValueStore struct
pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
    log: Arc<RwLock<Vec<String>>>,
    commit_log: Arc<RwLock<Vec<String>>>,
}

impl KeyValueStore {
    // Implement methods to interact with the store (set, get, update, delete, etc.)
    pub fn new() -> Self {
        KeyValueStore {
            data: Arc::new(RwLock::new(HashMap::new())),
            log: Arc::new(RwLock::new(Vec::new())),
            commit_log: Arc::new(RwLock::new(Vec::new())),
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
        self.log.write().unwrap().push(format!("SET {} {}", key, value));
    }

    pub fn delete(&self, key: &str)  {
        let data = self.data.write().unwrap();
        if data.contains_key(key) {
            println!("Key found in DELETE - {}", key);
            self.log.write().unwrap().push(format!("DELETE {}", key));
        } else {
            println!("Key not found in DELETE");
        }
    }

    pub fn append(&self, key: &str, value: String) {
        let data = self.data.write().unwrap();

        if data.contains_key(key) {
            println!("Key found in APPEND - {}", key);
            self.log.write().unwrap().push(format!("APPEND {} {}", key, value));
        } else {
            println!("Key not found in APPEND");
        }

    }

    pub fn commit(&self) {
        let mut data = self.data.write().unwrap();
        let mut log = self.log.write().unwrap();
        let mut commit_log = self.commit_log.write().unwrap();

        for entry in log.iter() {
            let mut split = entry.split_whitespace();
            let command = split.next().unwrap();
            let key = split.next().unwrap();
    
            match command {
                "SET" => {
                    let value = split.next().unwrap();
                    data.insert(key.to_string(), value.to_string());
                    commit_log.push(entry.to_string());
                    println!("Commited SET {} {}", key, value);
                },

                "DELETE" => {
                    data.remove(key);
                    commit_log.push(entry.to_string());
                    println!("Commited DELETE {}", key);
                },

                "APPEND" => {
                    let value = split.next().unwrap();
                    if let Some(current_value) = data.get_mut(key) {
                        current_value.push_str(&value);
                        commit_log.push(entry.to_string());
                        println!("Commited APPEND {} {}", key, value);
                    }
                },
                _ => {
                    println!("Unknown command");
                }
            }
        }
        log.clear();
    }

    pub fn get_commit_log(&self) -> Vec<String> {
        let log = self.commit_log.read().unwrap();
        log.clone()
    }

    pub fn get_log(&self) -> Vec<String> {
        let log = self.log.read().unwrap();
        log.clone()
    }

    pub fn get_data(&self) -> HashMap<String, String> {
        let data = self.data.read().unwrap();
        data.clone()
    }
    
}