use noler::node::NolerClient;
use noler::utility::parse_configuration;

use clap::{Arg, App};
use std::time::Instant;

#[tokio::main]
async fn main() {
    let matches = App::new("Noler Client")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("requests")
            .short('n')
            .long("requests")
            .value_name("REQUESTS")
            .help("Number of requests per client thread")
            .default_value("1"))
        .arg(Arg::with_name("threads")
            .short('t')
            .long("threads")
            .value_name("THREADS")
            .help("Number of client threads")
            .default_value("1"))
        .get_matches();

    let threads = matches.value_of("threads").unwrap().parse::<u32>().unwrap();
    let requests = matches.value_of("requests").unwrap().parse::<u32>().unwrap();
    let config_file = matches.value_of("config").unwrap();

    let handles = (0..threads).map(|_| {
        let config = parse_configuration(config_file);

        thread::spawn(move || {
            let client = NolerClient::new(0, config.clone());
            let request_start = Instant::now();

            match client.start_noler_client(requests) {
                Ok(_) => {
                    let request_end = Instant::now();
                    println!("Client ?: {} requests took {}ms", requests, request_end.duration_since(request_start).as_millis());
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                }
            }
        })
    }).collect::<Vec<_>>();

    // Wait for all threads to complete
    for handle in handles {
        handle.unwrap();
    }
}