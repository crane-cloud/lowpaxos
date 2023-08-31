use std::io::{BufRead, BufReader};
use std::fs::File;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::config::{Config, Replica};
use crate::message::MessageWrapper;

pub fn parse_configuration(config_file: &str) -> Config {
    let file = File::open(config_file).expect("Failed to open config file");
    let reader = BufReader::new(file);

    let mut f: isize = -1;
    let mut replicas = Vec::new();

    println!("Parsing configuration file: {}", config_file);

    for (line_index, line) in reader.lines().enumerate() {
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
                        if let Ok(socket_addr) = SocketAddr::from_str(arg) {
                            let replica = Replica::new(line_index as u32, socket_addr);
                            replicas.push(replica);
                        } else {
                            eprintln!("Failed to parse socket address: {}", arg);
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
    
    Config::new(0, n, f as usize, replicas)
}

pub fn wrap_and_serialize(meta: &str, data: String) -> String {
    let wrapper = MessageWrapper {
        msg_type: meta.to_string(),
        msg_content: data,
    };

    let serialized_meta = serde_json::to_string(&wrapper).unwrap();
    serialized_meta
}
