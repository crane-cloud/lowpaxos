use std::thread;
use std::net::SocketAddr;
use clap::Arg;

use noler::transport::Transport;
use noler::node::NolerReplica;
use noler::utility::{parse_configuration, parse_profile_file};
use noler::monitor::{Profile, ProfileMatrix};

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
        .arg(Arg::with_name("profiles")
            .short('p')
            .long("profiles")
            .value_name("FILE")
            .help("Profile configurations of the nodes")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("monitor")
            .short('m')
            .long("monitor")
            .value_name("MONITOR")
            .help("Sets the address of the monitor node")
            .takes_value(true)
            .required(true))
        .get_matches();

    let config_file = matches.value_of("config").unwrap();
    let profile_file = matches.value_of("profiles").unwrap();
    
    let id = matches.value_of("id").unwrap().parse::<u32>().unwrap();
    let monitor_address = matches.value_of("monitor").unwrap().parse::<SocketAddr>().unwrap();

    let config = parse_configuration(config_file);
    let matrix = parse_profile_file(config.replicas.len(), profile_file);

    /////////////////////////////CloudLab version/////////////////////////////

    //Get address of the replica from the config file using the id
    let replica_address = config.replicas.iter().find(|replica| replica.id == id).unwrap().replica_address.clone();
    let transport = Transport::new(replica_address);

    let profiles = matrix.get_row((id - 1) as usize).unwrap().clone();

    //let mut noler_replica = NolerReplica::new(id, replica_address, config.clone(), profiles.clone(), transport);
    let mut noler_replica = NolerReplica::new(id, replica_address, config.clone(), profiles, monitor_address, transport);

    noler_replica.start_noler_replica();


    /////////////////////////////Threaded version/////////////////////////////
    // let mut handles_replica = vec![];

    // for replica in config.replicas.iter() {

    //     let transport = Transport::new(replica.replica_address);

    //     //Get th profiles for this replica
    //     let profiles = matrix.get_row((replica.id - 1) as usize).unwrap().clone();


    //     let mut noler_replica = NolerReplica::new(replica.id, replica.replica_address, config.clone(), profiles, monitor_address,  transport);
    //     let handle_replica = thread::spawn(move || {
    //         noler_replica.start_noler_replica();
    //     });
    //     handles_replica.push(handle_replica);

    // }

    // handles_replica.into_iter().for_each(|handle| {
    //     handle.join().unwrap();
    // });
}