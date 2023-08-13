use paxos::Client;
use std::net::SocketAddr;
use std::str::FromStr;
use clap::{Arg, App};
use std::time::Instant;

#[tokio::main]
async fn main() {
    let matches = App::new("Paxos Client")
        .arg(Arg::with_name("clients")
            .short('c')
            .long("clients")
            .value_name("CLIENTS")
            .help("Number of client threads to spawn")
            .default_value("1"))
        .arg(Arg::with_name("threads")
            .short('t')
            .long("threads")
            .value_name("THREADS")
            .help("Number of client threads to spawn")
            .default_value("1"))
        .arg(Arg::with_name("requests")
            .short('r')
            .long("requests")
            .value_name("REQUESTS")
            .help("Number of requests per client thread")
            .default_value("1"))
        .get_matches();

    let clients = matches.value_of("clients").unwrap().parse::<u32>().unwrap();
    let threads = matches.value_of("threads").unwrap().parse::<u32>().unwrap();
    let requests = matches.value_of("requests").unwrap().parse::<u32>().unwrap();
    

    let addresses = vec!["127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"];
    let replicas = addresses
        .iter()
        .map(|addr| SocketAddr::from_str(addr).unwrap())
        .collect::<Vec<SocketAddr>>();

    let handles = (0..threads).map(|_| {
        let replicas = replicas.clone();
        tokio::spawn(async move {
            for _ in 0..clients {
                let client = Client::new(replicas.clone());
                let request_start = Instant::now();

                match client.await.run(requests).await {
                    Ok(_) => {
                        let request_end = Instant::now();
                        println!("Client X: {} requests took {}ms", requests, request_end.duration_since(request_start).as_millis());
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                    }
                }
            }
        })
    }).collect::<Vec<_>>();

    // Wait for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }

}
