use noler::utility::parse_configuration;
use noler::transport::TransportTimed;
use noler::config::Config;
use noler::message::{RequestMessage, ResponseMessage, MessageWrapper};
use common::utility::wrap_and_serialize;
use kvstore::{KvStoreMsg, Operation};

use clap::{Arg, App};
use std::time::Instant;
use std::thread;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use rand::Rng;
use rand::prelude::*;
use rand_distr::Zipf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs::OpenOptions;
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
use local_ip_address::list_afinet_netifas;

#[derive(Debug, Copy, Clone)]
struct RequestInfo {
    sent_instant: Instant,
    response_instant: Option<Instant>,
    result: Option<bool>,
}

impl RequestInfo {
    fn new() -> RequestInfo {
        RequestInfo {
            sent_instant: Instant::now(),
            response_instant: None,
            result: None,
        }
    }
}

#[derive(Debug)]
pub struct NolerClient {
    id: u32,
    transport: Arc<TransportTimed>,
    config: Config,
    leader: Arc<Mutex<Option<SocketAddr>>>,
    request_info: Arc<Mutex<Vec<RequestInfo>>>,
    log: Arc<Mutex<File>>,
}

impl NolerClient {
    fn new(id: u32, transport: TransportTimed, config: Config, log: File) -> NolerClient {

        NolerClient {
            id,
            transport: Arc::new(transport),
            config: config,
            leader: Arc::new(Mutex::new(None)),
            request_info: Arc::new(Mutex::new(Vec::new())),
            log: Arc::new(Mutex::new(log)),
        }
    }

    fn send_request(&mut self, op: &[u8]) -> bool {
        
        let leader = Arc::clone(&self.leader);

        let known_leader = {
            let leader = leader.lock().unwrap();
            leader.clone()
        };

        if let Some(known_leader) = known_leader {

             //Send the request to the leader
            match self.transport.send(
                &known_leader,
                op,
            ) {
                Ok(_) => {
                    //println!("Client {}: Sending request to leader {:?}", self.id, known_leader);
                    //println!("Client {}: Request sent to the known leader", self.id);
                    true
                },
                Err(e) => {
                    eprintln!("Client: Error: {}", e);
                    false
                }
            }

        }

        else {

            //Send the request to the first replica
            match self.transport.send(
                &self.config.replicas[0].replica_address, //Send to the first replica
                op,
            ) {
                Ok(_) => {
                    //println!("Client {}: Sending request to leader {:?}", self.id, known_leader);
                    //println!("Client {}: Request sent to the first replica", self.id);
                    true
                },
                Err(e) => {
                    eprintln!("Client: Error: {}", e);
                    false
                }
            }
        }

    }

    fn prepare_operation(&mut self, request_id: u64, request: i64, is_set: bool) -> String {
        // Update the request_info
        {
            let mut request_info = self.request_info.lock().unwrap();
            request_info.push(RequestInfo::new());
        }

        let op = if is_set {
            Operation::SET(request.to_string(), request_id.to_string())
        } else {
            Operation::GET(request.to_string())
        };

        let request_message = RequestMessage {
            client_id: self.transport.local_address(),
            request_id: request_id,
            operation: op.to_bytes().unwrap(),
        };

        let serialized_request = wrap_and_serialize(
            "RequestMessage",
            serde_json::to_string(&request_message).unwrap(),
        );

        serialized_request

    }


    fn send_requests(&mut self, request_vec: Vec<i64>, op_type: Vec<bool>) {
        let mut request_id = 0;

        println!("Start time: {}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());

        for (request, is_set) in request_vec.iter().zip(op_type.iter()) {
            request_id += 1;

            //Prepare the operation
            let prepared_operation = self.prepare_operation(request_id, *request, *is_set);

            //Send the operation to the replica system
            let mut send_operation  = self.send_request(&prepared_operation.as_bytes());

            //While the send operation is unsuccessful, keep trying
            while !send_operation {
                send_operation = self.send_request(&prepared_operation.as_bytes());
            }

            //Process the response
            let result = self.process_response();

            match result {
                Ok(result) => {
                    if result != request_id {
                        eprintln!("Client: Error: Request id {} was unsuccessful", request_id);
                        //Resend the request
                        //self.send_request(&serialized_request.as_bytes());
                    }

                    else {
                        continue;
                    }
                },
                Err(e) => {
                    eprintln!("Client: Receive Error: {}", e);
                    continue;
                }
                
            }
        }
    }


    fn process_response(&mut self) -> Result<u64, std::io::Error> {
        let request_info = Arc::clone(&self.request_info);
        let leader = Arc::clone(&self.leader);
        let transport = Arc::clone(&self.transport);
        let log = Arc::clone(&self.log);

        let mut buf = [0; 1024];

        match transport.receive_from(&mut buf) {
            Ok((len, from)) => {
                let message = String::from_utf8_lossy(&buf[..len]).to_string();

                //validate the response
                if is_valid_response(&message) {
                    //println!("Response time: {}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());

                    {
                        let mut leader = leader.lock().unwrap();
                        //Check if the leader on the client

                        if *leader == None {
                            //Update the leader with the replica that responds
                            *leader = Some(from);
                        }

                        else {
                            //Check if the response is from a leader we don't know
                            if Some(from) != *leader {
                                //We have a new leader - update
                                *leader = Some(from);
                            }
                        }
                    }

                    //Process the response here
                    let wrapped_response = serde_json::from_str::<MessageWrapper>(&message).unwrap();
                    let _wrapper = wrapped_response.msg_type.as_str(); //Already verified a ResponseMessage type
                    let response: ResponseMessage = serde_json::from_str(&wrapped_response.msg_content).unwrap();

                    let request_id = response.request_id;

                    let result:Result<KvStoreMsg<String, String>, _> = bincode::deserialize(&response.reply);

                    match result {
                        Ok(_msg) => {
                            //println!("Client: received reply: {:?} with id {}", msg, request_id); //Can get the successful read/writes here

                            {
                                //Acquire the lock to update the request_info
                                let mut request_info = request_info.lock().unwrap();

                                //Try to get the request_info for the request_id
                                let request_info = &mut request_info[(request_id as usize) - 1];

                                //Confirm that the request_id is correct and no response has been received before
                                if request_info.response_instant == None {
                                    request_info.response_instant = Some(Instant::now());
                                    request_info.result = Some(true);

                                    //Get the latency for the request
                                    let latency = request_info.response_instant.unwrap().duration_since(request_info.sent_instant);
                                    //println!("Round {} took {}ms", request_id, latency.as_millis());

                                    {
                                        let mut log = log.lock().unwrap();
                                        writeln!(log, "Round took {}ms", latency.as_millis()).expect("Unable to write to file");
                                    }

                                }
                                else {
                                    eprintln!("Client: Error: Request id {} already has a response", request_id);
                                }
                            }

                            Ok(request_id)
                        },
                        Err(e) => {
                            eprintln!("Client Error: - Message serialization {}", e);
                            Ok(0)
                        }
                    }
                } else {
                    eprintln!("Client Error - Invalid response: {:?}", message);
                    Ok(0)
                }
            }

            Err(err) => {
                eprintln!("Client Error - No response {}", err);
                Ok(0)
            }
        }
    }

}

fn run_noler_client(id: u32, config: Config, transport: SocketAddr, requests: u32, conflicts: u32, writes: u32, rounds: u32, log: String) {

    let read_timeout = Some(Duration::from_millis(150));
    let write_timeout = Some(Duration::from_millis(150));

    //Create the file name based on the client id
    let log = format!("{}_{}", log, id);

    //Create the log file based on the client id
    let log = File::create(log).expect("Unable to create the log file");

    let mut client = NolerClient::new(id,TransportTimed::new(transport, read_timeout, write_timeout), config, log);

    // Prepare the requests
    let (karray, put) = prepare_request(requests, rounds, conflicts, writes);

    // Start sending async requests
    client.send_requests(karray, put);

    //Print the end time
    //println!("End time: {}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()); //ToDo: Incorrect

    // Wait for the responses to be received
    thread::sleep(Duration::from_secs(1));

    // Determine the number of successful requests
    let mut successful_requests = 0;

    {
        let request_info = client.request_info.lock().unwrap();

        for request in request_info.iter() {
            if request.result == Some(true) {
                successful_requests += 1;
            }
        }

        println!("Client {}: {} out of {} requests were successful", id, successful_requests, requests);
    }
}


fn is_valid_response(response: &str) -> bool {
    // Attempt to parse the response as a JSON object
    if let Ok(parsed_response) = serde_json::from_str::<serde_json::Value>(response) {
        // Check if the parsed JSON object has the expected structure
        if let Some(msg_type) = parsed_response.get("msg_type") {
            if msg_type == "ResponseMessage" {
                return true;
            }
        }
    }

    false
}

fn create_transport(id: u32) -> SocketAddr {

    let network_interfaces = list_afinet_netifas();

    if let Ok(network_interfaces) = network_interfaces {
        for (_, ip) in network_interfaces.iter() {

            if let IpAddr::V4(ipv4_addr) = ip {
                let ip4addr: Ipv4Addr = *ipv4_addr;

                if ip4addr.octets()[0] == 10 && ip4addr.octets()[1] == 10 {
                    let ip = IpAddr::V4(ip4addr);
                    return SocketAddr::new(ip, 3000 + id as u16);
                }
            }
        }
    }
    else {
        eprintln!("Error: {}", network_interfaces.unwrap_err());
        
    }

    // Use the localhost settings if the network interface is not found
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)); //Localhost
    return SocketAddr::new(ip, 3000 + id as u16);

}

fn prepare_request(requests: u32, rounds: u32, conflicts: u32, writes: u32) -> (Vec<i64>, Vec<bool>) {
    let mut rng = rand::thread_rng();

    // Create Zipf distribution
    let zipf = Zipf::new(1000, 1.5).unwrap();

    // Initialize the arrays
    let mut karray = vec![0; (requests / rounds) as usize];
    let mut put = vec![false; (requests / rounds) as usize];

    for i in 0..requests as usize {
        if conflicts > 0 {
            let r: u32 = rng.gen_range(0..100);

            if r < conflicts {
                // In the case of conflicts, use the same key
                karray[i] = 42;
            } else {
                karray[i] = (43 + i) as i64;
            }

            let r = rng.gen_range(0..100);

            if r < writes {
                put[i] = true;
            } else {
                put[i] = false;
            }
        } else {
            put[i] = true; // ToDo: Comment out
            karray[i] = zipf.sample(&mut rng) as i64;
        }
    }

    // Print the populated arrays for demo
    // println!("karray: {:?}", karray);
    // println!("put: {:?}", put);

    (karray, put)
}

fn main() {
    let matches = App::new("Noler Client")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("threads")
            .short('t')
            .long("threads")
            .value_name("THREADS")
            .help("Number of client threads")
            .default_value("1"))
        .arg(Arg::with_name("requests")
            .short('n')
            .long("requests")
            .value_name("REQUESTS")
            .help("Number of requests per client thread")
            .default_value("1"))
        .arg(Arg::with_name("file")
            .short('f')
            .long("file")
            .value_name("FILE")
            .help("Name of the file to write the log to")
            .default_value("log.out"))
        .arg(Arg::with_name("conflicts")
            .short('k')
            .long("conflicts")
            .value_name("CONFLICTS")
            .help("Percentage of conflicts")
            .default_value("0"))
        .arg(Arg::with_name("writes")
            .short('w')
            .long("writes")
            .value_name("WRITES")
            .help("Percentage of writes")
            .default_value("0"))
        .arg(Arg::with_name("rounds")
            .short('r')
            .long("rounds")
            .value_name("ROUNDS")
            .help("Number of rounds")
            .default_value("1"))
        .get_matches();

    let requests = matches.value_of("requests").unwrap().parse::<u32>().unwrap();
    let conflicts = matches.value_of("conflicts").unwrap().parse::<u32>().unwrap();
    let writes = matches.value_of("writes").unwrap().parse::<u32>().unwrap();
    let rounds = matches.value_of("rounds").unwrap().parse::<u32>().unwrap();
    let threads = matches.value_of("threads").unwrap().parse::<u32>().unwrap(); //Number of clients

    let log = matches.value_of("file").unwrap().to_string();

    let config_file = matches.value_of("config").unwrap();

    for id in 0..threads {
        let transport = create_transport(id);

        let config = parse_configuration(config_file);
        let log = log.clone();

        run_noler_client(id, config, transport, requests, conflicts, writes, rounds, log);
    }

    thread::sleep(Duration::from_secs(1));

}