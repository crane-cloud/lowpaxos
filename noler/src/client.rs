use noler::utility::parse_configuration;
//use noler::transport::Transport;
use noler::config::Config;
use noler::message::{RequestMessage, ResponseMessage, MessageWrapper};
use common::utility::{wrap_and_serialize, generate_random_key};
use kvstore::{KvStoreMsg, Operation};


use clap::{Arg, App};
//use std::time::Instant;
use std::thread;
//use std::sync::Arc;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};
use std::thread::sleep;
use rand::Rng;
//use rand::distributions::Distribution;
use rand::prelude::*;
use rand_distr::Zipf;
use rand_distr::{Poisson, Distribution};
use std::error::Error;
use std::fs::File;
use std::io::Write;
use serde::{Serialize, Deserialize};

pub struct NolerClient {
    id: u32,
    address: SocketAddr,
    config: Config,
    //transport: Arc<Transport>,
    socket: UdpSocket,
    req_id: u64,
    //leader: Option<SocketAddr>,
}

impl NolerClient {
    fn new(id: u32, address: SocketAddr, config: Config) -> NolerClient {
        let socket = UdpSocket::bind(address).unwrap();
        NolerClient {
            id,
            address,
            config: config,
            //transport: Arc::new(transport),
            socket,
            req_id: 0,
            //leader: None,
        }
    }

    pub fn start_noler_client(&mut self, requests: u32, conflicts: u32, writes: u32, rounds: u32, file: String) {
        println!("{}: starting the noler client", self.id);

        //Create the log file
        let mut file = File::create(file).unwrap();

        // let mut rng = rand::thread_rng();
        // let zipf = zipf::ZipfDistribution::new(10000, 2.5).unwrap();
        // let sample = zipf.sample(&mut rng);
        // println!("Sample: {}", sample);


        // let val: f64 = thread_rng().sample(Zipf::new(10000, 2.5).unwrap());
        // println!("Sample: {}", val);

        // let poi = Poisson::new(100.0).unwrap();
        // let v = poi.sample(&mut rand::thread_rng());
        // println!("{} is from a Poisson(2) distribution", v);

        //Generate keys equal to the number of requests

        let mut rng = rand::thread_rng();

        //Create Zipf distribution
        let zipf = Zipf::new(1000, 1.5).unwrap();

        //Initilaize the arrays
        let mut karray = vec![0; (requests/rounds) as usize];
        let mut put = vec![false; (requests/rounds) as usize];



        for i in 0..requests as usize {

            if conflicts > 0 {
                let r: u32 = rng.gen_range(0..100);

                if r < conflicts {
                    //In the case of conflicts, use the same key
                    karray[i] = 42;
                }
                else {
                    karray[i] = (43 + i) as i64;
                }

                let r = rng.gen_range(0..100);

                if r < writes {
                    put[i] = true;
                }

                else {
                    put[i] = false;
                }
            }

            else {
                karray[i] = zipf.sample(&mut rng) as i64;
            }

        }

        //Print the populated arrays for demo
        println!("karray: {:?}", karray);
        println!("put: {:?}", put);


        write!(file, "Start time: ").expect("Unable to write to file");
        //let before_total_instant = Instant::now();
        let before_total = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").subsec_nanos();
        writeln!(file, "{}", before_total).expect("Unable to write to file");



        let mut success = 0;
        let mut latencies = Vec::<Option<Duration>>::new();

        for i in 0..requests {
            let operation = if put[i as usize] {
                Operation::SET(karray[i as usize].to_string(), i.to_string())
            } else {
                Operation::GET(karray[i as usize].to_string())
            };

            println!("Client {}: Sending request {:?}", self.id, operation);

            match self.send_request(operation.to_bytes().unwrap()) {
                Ok(Some(latency)) => {
                    writeln!(file, "{}", latency.as_nanos()).expect("Unable to write to file");
                    success += 1;
                    latencies.push(Some(latency))
                },
                Ok(None) => {
                    println!("Client {}: Request failed", self.id);
                    latencies.push(None)
                },
                Err(e) => {
                    println!("Client {}: Error: {}", self.id, e);
                    latencies.push(None)
                }
            }
        }
        ////////////////////////////////////////////////////////////////////////////
        write!(file, "End time: ").expect("Unable to write to file");
        let after_total = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").subsec_nanos();
        writeln!(file, "{}", after_total).expect("Unable to write to file");
        

        //Calculate and print the statistics
        // let average_latency = calculate_average_latency(&latencies);
        // let min_latency = calculate_min_latency(&latencies);
        // let max_latency = calculate_max_latency(&latencies);
        // let percentile_latency = calculate_percentile_latency(&latencies, 99.0); // Calculate the 95th percentile latency

        // println!("Minimum Latency: {:?}", min_latency);
        // println!("Maximum Latency: {:?}", max_latency);
        // println!("99th Percentile Latency: {:?}", percentile_latency);
        // println!("Client: {} requests took {}ms", requests, average_latency.unwrap().as_millis());

        println!("Client: {} requests were successful", success);

    }

        fn send_request(&mut self, op: Vec<u8>) -> Result<Option<Duration>, Box<dyn Error>> {
            self.req_id += 1;

            let request_message = RequestMessage {
                client_id: self.address,
                request_id: self.req_id,
                operation: op,
            };

            let serialized_request = wrap_and_serialize(
                "RequestMessage", 
                serde_json::to_string(&request_message).unwrap()
            );

            let start_time = Instant::now();

            self.socket.send_to(
                &mut serialized_request.as_bytes(),
                &self.config.replicas[0].replica_address,
            ).expect("Client: Failed to send request to replica");

            let mut response_buffer = [0; 1024];
            let (bytes_received, _from) = self.socket.recv_from(&mut response_buffer)?;

            let wrapped_response = serde_json::from_str::<MessageWrapper>(&String::from_utf8_lossy(&response_buffer[..bytes_received]).to_string()).unwrap();

            let wrapper = wrapped_response.msg_type.as_str();

            match wrapper {
                "ResponseMessage" => {
                    let response = wrapped_response.msg_content;
                    let r = serde_json::from_str::<ResponseMessage>(&response).unwrap();

                    //let deserialized_r: Vec<u8> = bincode::deserialize(&r.reply).unwrap();

                    let result:Result<KvStoreMsg<String, String>, _> = bincode::deserialize(&r.reply);

                    match result {
                        Ok(msg) => {
                            //println!("Client: Received reply: {:?}", msg); //Can get the successful read/writes here

                            match msg {
                                KvStoreMsg::SetOk(key) => {
                                    println!("Client: SET {} was successful", key);
                                },
                                KvStoreMsg::GetOk(key, value) => {
                                    println!("Client: GET {} {} was successful", key, value);
                                },
                                KvStoreMsg::None => {
                                    println!("Client: Operation was unsuccessful");
                                },
                            }

                            if self.req_id == r.request_id {
                                let end_time = Instant::now();
                                let latency = end_time.duration_since(start_time);
                                return Ok(Some(latency));
                            }

                            else {
                                return Ok(None)
                            }
                        },
                        Err(e) => {
                            println!("Error: {}", e);
                            return Ok(None)
                        }
                    }
                },
                _ => {
                    println!("Unknown message type: {}", wrapper);
                    return Ok(None);
                }
            }
        }

}

fn calculate_average_latency(latencies: &[Option<Duration>]) -> Option<Duration> {
    let (total_latency, count) = latencies.iter().fold(
        (Duration::from_secs(0), 0),
        |(acc, count), item| match item {
            Some(duration) => (acc + *duration, count + 1),
            None => (acc, count),
        },
    );

    if count > 0 {
        Some(total_latency / count as u32)
    } else {
        None
    }
}

fn calculate_min_latency(latencies: &[Option<Duration>]) -> Option<Duration> {
    latencies
        .iter()
        .filter_map(|&latency| latency)
        .min()
}

fn calculate_max_latency(latencies: &[Option<Duration>]) -> Option<Duration> {
    latencies
        .iter()
        .filter_map(|&latency| latency)
        .max()
}

fn calculate_percentile_latency(latencies: &[Option<Duration>], percentile: f32) -> Option<Duration> {
    let mut latencies = latencies
        .iter()
        .filter_map(|&latency| latency)
        .collect::<Vec<Duration>>();

    latencies.sort();
    let index = (percentile / 100.0 * latencies.len() as f32) as usize;
    latencies.get(index).cloned()
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
        .arg(Arg::with_name("file")
            .short('f')
            .long("file")
            .value_name("FILE")
            .help("Name of the file to write the log to")
            .default_value("latencies.txt"))
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

    let threads = matches.value_of("threads").unwrap().parse::<u32>().unwrap();
    let requests = matches.value_of("requests").unwrap().parse::<u32>().unwrap();
    let conflicts = matches.value_of("conflicts").unwrap().parse::<u32>().unwrap();
    let writes = matches.value_of("writes").unwrap().parse::<u32>().unwrap();
    let rounds = matches.value_of("rounds").unwrap().parse::<u32>().unwrap();
    

    let log = matches.value_of("file").unwrap().to_string();
    let config_file = matches.value_of("config").unwrap();

    let address = "127.0.0.1:32000".parse().unwrap();

    let handles = (0..threads).map(|_| {
        let config = parse_configuration(config_file);
        let log = log.clone();

        thread::spawn(move || {
            let mut client = NolerClient::new(0, address, config);
            client.start_noler_client(requests, conflicts, writes, rounds,  log);
        })
    }).collect::<Vec<_>>();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
}