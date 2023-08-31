use std::thread;

use clap::Arg;
use crossbeam_channel::{unbounded, Receiver, Sender};

use noler::transport::Transport;
use noler::node::NolerReplica;
use noler::utility::parse_configuration;

//#[tokio::main]
fn main() {

    let matches = clap::App::new("VR Replica")
        .version("0.1.0")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true)
            .required(true))
        .get_matches();

    let config_file = matches.value_of("config").unwrap();

    let config = parse_configuration(config_file);

    let (tx, rx): (Sender<String>, Receiver<String>) = unbounded();

    //let mut tasks = vec![];

    let mut handles_replica = vec![];
    //let mut handles_rcv = vec![];

    for replica in config.replicas.iter() {

        let transport = Transport::new(replica.replica_address);

        // let socket_tx = tx.clone();

        // let handle_rcv = thread::spawn(move || {
        //     receive_and_send(&transport, &socket_tx);
        // });
        // handles_rcv.push(handle_rcv);

        let mut noler_replica = NolerReplica::new(replica.id, replica.replica_address, config.clone(), transport, tx.clone(), rx.clone());
        let handle_replica = thread::spawn(move || {
            noler_replica.start_noler_replica();
        });
        handles_replica.push(handle_replica);

    }

    handles_replica.into_iter().for_each(|handle| {
        handle.join().unwrap();
    });

    // handles_rcv.into_iter().for_each(|handle| {
    //     handle.join().unwrap();
    // });

}


fn receive_and_send(socket: &Transport, tx: &Sender<String>) {
    let mut buffer = [0u8; 1024]; // Adjust buffer size as needed

    loop {
        match socket.receive_from(&mut buffer) {
            Ok((size, _)) => {
                let message = String::from_utf8_lossy(&buffer[..size]).to_string();
                tx.send(message).unwrap();
            }
            Err(err) => {
                eprintln!("Error receiving from socket: {}", err);
            }
        }
    }
}