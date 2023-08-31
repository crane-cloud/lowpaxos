use std::io::{BufRead, BufReader};
use std::fs::File;
use crate::configuration::{Config, ReplicaAddress};
use crate::message::MessageWrapper;

pub fn parse_configuration(config_file: &str) -> Config {
    let file = File::open(config_file).expect("Failed to open config file");
    let reader = BufReader::new(file);

    let mut f:isize = -1;
    let mut replicas = Vec::new();

    for line in reader.lines() {

        if let Ok(line) = line {
            let tokens: Vec<_> = line.split_whitespace().collect();
            if tokens.is_empty() || tokens[0] == "#" {
                continue; // Skip comments and empty lines
            }

            match tokens[0] {
                "f" => {
                    if let Some(arg) = tokens.get(1) {
                        f = arg.parse().expect("'f' configuration line requires a valid integer argument");
                    }
                }
                "replica" => {
                    if let Some(arg) = tokens.get(1) {
                        let parts: Vec<_> = arg.split(':').collect();
                        if parts.len() == 2 {
                            let replica = ReplicaAddress::new(parts[0], parts[1]);
                            replicas.push(replica);
                        }
                    }
                }
                _ => {
                    panic!("Unknown configuration directive: {}", tokens[0]);
                }
            }
        }
    }

    let n = replicas.len();
    if n == 0 {
        panic!("Configuration did not specify any replicas");
    }

    if f == -1 {
        panic!("Configuration did not specify an 'f' parameter");
    }
    
    Config::new(n, f as usize, replicas)

}

pub fn wrap_and_serialize(meta: &str, data: String) -> String {
    //let serialized_message = serde_json::to_string(&data).unwrap();

    let wrapper = MessageWrapper {
        msg_type: meta.to_string(),
        msg_content: data,
    };

    let serialized_meta = serde_json::to_string(&wrapper).unwrap();

    serialized_meta
}