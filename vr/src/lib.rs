// pub mod view_stamped {
//     tonic::include_proto!("view_stamped");
// }

extern crate kvstore;
extern crate common;

pub mod  configuration;
pub mod types;
pub mod node;
pub mod logging;
pub mod messages;
pub mod utility;

pub mod kvlog;

pub mod transport;

pub mod quorum;
// mod latency;
// mod messages;
// mod timeout;
// mod lplog;
// mod types;
// mod client;

// mod node;
// mod configuration;
// mod utility;

// pub use node::{VsrReplica, VsrClient};
// pub use configuration::{Config, ReplicaAddress};
// pub use utility::parse_configuration;