use std::thread;

use clap::Arg;

use noler::transport::Transport;
use noler::node::NolerReplica;
use noler::utility::parse_configuration;

fn main() {

    let matches = clap::App::new("Noler Replica")
        .version("0.1.0")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("id")
            .short('i')
            .long("id")
            .value_name("ID")
            .help("Sets the id of the replica")
            .takes_value(true)
            .required(true))
        .get_matches();

    let config_file = matches.value_of("config").unwrap();
    let id = matches.value_of("id").unwrap().parse::<u32>().unwrap();

    let config = parse_configuration(config_file);

    /////////////////////////////CloudLab version/////////////////////////////

    //Get address of the replica from the config file using the id
    let replica_address = config.replicas.iter().find(|replica| replica.id == id).unwrap().replica_address.clone();
    let transport = Transport::new(replica_address);

    let mut noler_replica = NolerReplica::new(id, replica_address, config.clone(), transport);

    noler_replica.start_noler_replica();


    /////////////////////////////Threaded version/////////////////////////////
    // let mut handles_replica = vec![];

    // for replica in config.replicas.iter() {

    //     let transport = Transport::new(replica.replica_address);


    //     let mut noler_replica = NolerReplica::new(replica.id, replica.replica_address, config.clone(), transport);
    //     let handle_replica = thread::spawn(move || {
    //         noler_replica.start_noler_replica();
    //     });
    //     handles_replica.push(handle_replica);

    // }

    // handles_replica.into_iter().for_each(|handle| {
    //     handle.join().unwrap();
    // });
}