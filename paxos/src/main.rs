use paxos::PaxosNode;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let addresses = vec!["127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"];

    let replicas: Arc<Vec<SocketAddr>> = Arc::new(addresses
        .iter()
        .map(|s| SocketAddr::from_str(s).unwrap())
        .collect(),
    );

    for (id, replica) in replicas.iter().enumerate() {
        let replica = PaxosNode::new(id as u32 + 1, *replica, replicas.clone()); // Pass the clone into PaxosNode::new

        tokio::spawn(async move {
            replica.await.start_paxos().await;
        });
    }

    // Keep the main thread alive to allow the spawned nodes to run concurrently.
    tokio::signal::ctrl_c().await.unwrap();
}
