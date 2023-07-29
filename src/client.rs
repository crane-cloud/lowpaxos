use lowpaxos::Client;

#[tokio::main]
async fn main() {
    let peers = vec!["127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut client = Client::new(peers, "127.0.0.1:32000").await;
    client.run().await;
}
