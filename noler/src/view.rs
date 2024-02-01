use noler::monitor::ProfileMatrix;
use noler::transport::Transport;
use noler::utility::{parse_configuration, parse_profile_file};
use noler::message::{MessageWrapper, RequestProfileMessage, ResponseProfileMessage};

use std::net::SocketAddr;
use clap::{Arg, App};
use std::net::{Ipv4Addr, IpAddr};

struct Monitor {
    transport: Transport,
    //config: Config,
    matrix: ProfileMatrix,
}

impl Monitor {
    fn new(transport: Transport, matrix: ProfileMatrix) -> Self {
        Self {
            transport,
            //config,
            matrix,
        }
    }

    fn handle_request_profile(&mut self, src: SocketAddr, request: RequestProfileMessage) {

        let id = request.id;
        let profiles = self.matrix.get_row((id - 1) as usize);

        if profiles.is_some() {
            println!("Sending profiles {:?} to {}", profiles, src);
            
            let response = ResponseProfileMessage {
                profiles: profiles.unwrap(),
            };

            let response_message = MessageWrapper {
                msg_type: "ResponseProfileMessage".to_string(),
                msg_content: serde_json::to_string(&response).unwrap(),
            };

            let response_message = serde_json::to_string(&response_message).unwrap();
            self.transport.send(&src, response_message.as_bytes()).expect("Failed to send response");

        } else {
            println!("No profile found for {}", id);
            return;
        }
    }

    fn run_monitor(&mut self) {
        loop {
            let mut buf = [0; 1024];

            let (len , src) = self.transport.receive_from(&mut buf).expect("Didn't receive data");
            let message = String::from_utf8(buf[..len].to_vec()).expect("Failed to convert to String");

            let wrapper: Result<MessageWrapper, _> = serde_json::from_str(&message);

            match wrapper {
                Ok(wrapper) => {
                    match wrapper.msg_type.as_str() {
                        "RequestProfileMessage" => {
                            let request: Result<RequestProfileMessage, _> = 
                                serde_json::from_str(&wrapper.msg_content);

                            match request {
                                Ok(request) => {
                                    self.handle_request_profile(src, request);
                                },
                                Err(err) => {
                                    println!("Failed to deserialize request message: {:?}", err);
                                    continue;
                                }
                            }  
                        },

                        _ => {
                            println!("Unknown message type: {}", wrapper.msg_type);
                            continue;
                        }
                    }
                },
                Err(err) => {
                    println!("Failed to deserialize message: {:?}", err);
                    continue;
                }

            }

        }
    }
}

fn main(){
    let matches = App::new("Noler Monitor")
    .arg(Arg::with_name("config")
        .short('c')
        .long("config")
        .value_name("FILE")
        .help("Sets a custom config file")
        .takes_value(true)
        .required(true))
    .arg(Arg::with_name("profiles")
        .short('p')
        .long("profiles")
        .value_name("FILE")
        .help("Profile configurations of the nodes")
        .takes_value(true)
        .required(true))
    .get_matches();

    let config_file = matches.value_of("config").unwrap();
    let profiles_file = matches.value_of("profiles").unwrap();

    let config = parse_configuration(config_file);
    let matrix = parse_profile_file(config.replicas.len(), profiles_file);

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 10, 1, 6)), 30000);
    let transport = Transport::new(address);
    let mut monitor = Monitor::new(transport, matrix);

    monitor.run_monitor();
}