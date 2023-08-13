extern crate kvstore;
extern crate common;

//use kvstore::KeyValueStore;

mod node;
mod message;
mod network;

pub use node::PaxosNode;
pub use node::ClientT;
pub use node::Client;
pub use message::PaxosMessage;
pub use network::{send_message,
                   receive_message,
                };