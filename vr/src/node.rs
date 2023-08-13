use std::collections::HashSet;
use tokio::net::UdpSocket;
//use std::time::Duration;
use rand::Rng;
// use tonic::codegen::http::request;
use crate::configuration::{Config, self};
//use crate::timeout::Timeout;
use crate::logging::VrLogger;
use crate::kvlog::{Log, LogEntryState};
use crate::messages::{ViewSrMessage as MessageType, *};
use crate::transport::Transport;
use crate::utility::wrap_and_serialize;
use crate::quorum::QuorumSet;
use tokio::time::{Duration, sleep};
use rand::distributions::Alphanumeric;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};


const VR_STATUS_NORMAL: u32 = 0;
const VR_STATUS_VIEW_CHANGE: u32 = 1;
const VR_STATUS_RECOVERING: u32 = 2;

static LOGGER: VrLogger = VrLogger;
// log::set_logger(&LOGGER).unwrap();
// log::set_max_level(LevelFilter::Trace);

pub struct VsrReplica {
    id: usize,
    status: u32, // 0: normal, 1: view change, 2: recovering
    view: u64, // view number
    last_op: u64, // most recent operation number, initialized to 0
    last_committed: u64, // most recent committed operation number, initialized to 0
    last_request_state_transfer_view: u64,
    last_request_state_transfer_opnum: u64,
    last_batch_end: u64,
    batch_complete: bool,
    // view_change_timeout: Timeout<impl FnMut() + Send>,
    // null_commit_timeuot: Timeout<impl FnMut() + Send>,
    // state_transfer_timeout: Timeout<impl FnMut() + Send>,
    // resend_prepare_timeout: Timeout<impl FnMut() + Send>,
    // close_batch_timeout: Timeout<impl FnMut() + Send>,
    // recovery_timeout: Timeout<impl FnMut() + Send>,
    prepare_ok_quorum: QuorumSet<(u64, u64), PrepareOkMessage>,
    start_view_change_quorum: HashSet<i32>,
    do_view_change_quorum: HashSet<i32>,
    recovery_response_quorum: HashSet<i32>,
    batch_size: u32,
    log: Log, // requests that have been received in their order
    //client_table: Vec<u64>, // client table - todo

    config: Config,
    transport: Transport,

}

impl VsrReplica {
    pub async fn new (
        config: configuration::Config, // Sorted array of all replica addresses
        idx: usize, // replica ID - index into the config array for this replica address
        initialize: bool,
        batch_size: u32,
    ) -> VsrReplica {
        // // Display configuration details
        // println!("Configuration: {:?}", config);
        // println!("Quorum Size: {}", config.quorum_size());
        // println!("Fast Quorum Size: {}", config.fast_quorum_size());
        // println!("Index ID: {}", idx);

        let addr:SocketAddr = format!("{}:{}", 
            config.replicas[idx as usize].host, config.replicas[idx as usize].port)
            .parse()
            .unwrap();

        let transport = Transport::new(addr).expect("Transport initialization failed");

        // let socket = UdpSocket::bind(addr).await.expect("Failed to bind socket");
        // println!("Listening on {}", addr);

        //let mut prepare_ok_quorum = QuorumSet::new(2); //should be f
        // let mut start_view_change_quorum = HashSet::new();
        // let mut do_view_change_quorum = HashSet::new();
        // let mut recover_response_quorum = HashSet::new();

        // prepare_ok_quorum.extend(0..config.quorum());
        // start_view_change_quorum.extend(0..config.quorum());
        // do_view_change_quorum.extend(0..config.quorum());
        // recover_response_quorum.extend(0..config.quorum());

        // let view_change_timeout = Timeout::new(Duration::from_millis(5000), || {
        //     log::warn!("Have not heard from leader; starting view change");
        //     let _ = self.start_view_change(self.view + 1);
        // });

        // let null_commit_timeout = Timeout::new(Duration::from_millis(1000), || {
        //     log::info!("Sending null commit");
        //     self.send_null_commit();
        // });

        // let state_transfer_timeout = Timeout::new(Duration::from_millis(1000), || {
        //     log::info!("Starting state transfer");
        //     self.last_request_state_transfer_view = 0;
        //     self.last_request_state_transfer_opnum = 0;
        // });

        // state_transfer_timeout.start();
        // let resend_prepare_timeout = Timeout::new(Duration::from_millis(500), || {
        //     ResendPrepare();
        // });

        // let close_batch_timeout = Timeout::new(Duration::from_millis(300), || {
        //     CloseBatch();
        // });

        // let recovery_timeout = Timeout::new(Duration::from_millis(5000), || {
        //     SendRecoveryMessages();
        // });

        // let status = if initialize {
        //     if am_leader(self.view, self.idx, &config) {
        //         null_commit_timeout.start();
        //     } else {
        //         view_change_timeout.start();
        //     }
        //     VR_STATUS_NORMAL
        // } else {
        //     VR_STATUS_RECOVERING
        // };

        VsrReplica {
            id: idx,
            status: VR_STATUS_NORMAL,
            view: 0,
            last_op: 0, //Todo: check this
            last_committed: 0,
            last_request_state_transfer_view: 0,
            last_request_state_transfer_opnum: 0,
            last_batch_end: 0,
            batch_complete: false,
            // view_change_timeout: Timeout::new(0),
            // null_commit_timeuot: Timeout::new(0),
            // state_transfer_timeout: Timeout::new(0),
            // resend_prepare_timeout: Timeout::new(0),
            // close_batch_timeout: Timeout::new(0),
            // recover_timeout: Timeout::new(0),
            prepare_ok_quorum: QuorumSet::new(config.quorum_size() as i32),
            start_view_change_quorum: HashSet::new(),
            do_view_change_quorum: HashSet::new(),
            recovery_response_quorum: HashSet::new(),
            batch_size: 0,
            log: Log::new(true, 1, "EMPTY"),
            config: config,
            transport: transport,
        }
    }

    fn handle_request_message (&mut self, request_message: RequestMessage) {

        if self.status == VR_STATUS_RECOVERING {
            log::warn!("Rejecting request while recovering");
            return;
        }

        if self.status == VR_STATUS_VIEW_CHANGE {
            log::warn!("Rejecting request while view changing");
            return;
        }

        if self.status == VR_STATUS_NORMAL {
            //log::info!("Received request {} from client {} with op {:?}", clientreqid, clientid, op);
            println!("Handling: {:?}", request_message);

            // Need to check the client table to confirm if new request

            // if yes, confirm I am a leader

            if am_leader(self.view, self.id, &self.config) {
                self.last_op += 1;
                println!("Request: Leader appending viewstamp <{},{}> to the log", self.view, self.last_op);
                
                self.log.append((self.view, self.last_op), request_message.request.clone(), LogEntryState::Request);

                let prepare = PrepareMessage {
                    view: self.view,
                    opnum: self.last_op,
                    batchstart: self.last_batch_end,
                    requests: vec![request_message.request],
                };

                let serialized_prepare = wrap_and_serialize(
                    "PrepareMessage", 
                    serde_json::to_string(&prepare).unwrap()
                );
                //Send prepare message to all replicas
                let _ = self.transport.broadcast(
                    &self.config, serialized_prepare.as_bytes()
                );

            } else {
                println!("Not leader");
                //Can provide the client with the leader address here
            }

        }
    }

    fn handle_prepare(&mut self, msg:PrepareMessage) {
        println!("Received PREPARE <{}, {}-{}>)", 
            msg.view, 
            msg.batchstart,
            msg.opnum
        );

        if self.status != VR_STATUS_NORMAL {
            println!("Rejecting PREPARE while not in normal state");
            return;
        }

        if msg.view > self.view {
            println!("Rejecting PREPARE with stale view");
            return;
        }

        if am_leader(self.view, self.id, &self.config) {
            println!("Rejecting PREPARE as leader");
            return;
        }

        //ToDo: What is the role of the asserts here?
        //assert!(msg.batchstart <= msg.opnum);
        //assert_eq!(msg.opnum - msg.batchstart + 1, msg.requests.len() as u64);

        //self.view_change_timeout.reset();

        if msg.opnum <= self.last_op {
            println!("Rejecting PREPARE with stale opnum - Resend");
            // Resend the prepare_ok message

            //return;
        }

        if msg.batchstart > self.last_op + 1 {
            println!("Rejecting PREPARE with gap in opnum - Resend");
            // Request for state transfer

            //return;
        }

        //let mut op = msg.batchstart as i32 - 1; //ToDo for batching
        let mut op = self.last_op as i32 - 1;

        for req in msg.requests.iter() {
            op += 1;
            if op < self.last_op as i32 {
                continue;
            }

            self.last_op += 1;

            println!("Replica: Appending to the log");
            self.log.append(
                (msg.view, self.last_op),
                req.clone(),
                LogEntryState::Prepare,
            );

            // self.update_client_table(
            //     req.clientid,
            //     req.clientreqid,
            //     self.view,
            //     self.last_op,
            //     LogEntryState::Prepared,
            // );
        }

        //assert_eq!(op, msg.opnum); //ToDo

        //Build reply and send to the leader

        let mut reply = PrepareOkMessage::default();
        reply.view = msg.view;
        reply.opnum = msg.opnum;
        reply.replicaidx = self.id as u32;

        let serialized_prepare_ok = wrap_and_serialize(
            "PrepareOkMessage", 
            serde_json::to_string(&reply).unwrap()
        );

        let leader = self.config.get_leader(msg.view as usize);

        let remote:SocketAddr = format!("{}:{}", 
        self.config.replicas[leader].host, self.config.replicas[leader].port)
        .parse()
        .unwrap();

        let _ = self.transport.send(&remote, serialized_prepare_ok.as_bytes());


    }

    fn handle_prepareok(&mut self, msg:PrepareOkMessage, from: SocketAddr) {
        println!("Received PREPAREOK <{}, {}-{}>)", 
            msg.view, 
            msg.opnum,
            msg.replicaidx,
        );

        if self.status != VR_STATUS_NORMAL {
            println!("Rejecting PREPAREOK while not in normal state");
            return;
        }

        if msg.view > self.view {
            println!("Rejecting PREPAREOK with stale view");
            //ToDo: Request for state transfer
            return;
        }

        if !am_leader(self.view, self.id, &self.config) {
            println!("Rejecting PREPAREOK - I am the leader");
            return;
        }

        let vs = (msg.view, msg.opnum);

        if let Some(msgs) = self.prepare_ok_quorum.add_and_check_for_quorum(vs, msg.replicaidx as i32, msg.clone())
        {
            // We have a quorum of PrepareOK messages for this opnumber.
            // Execute it and all previous operations.
            //
            // (Note that we might have already executed it. That's fine,
            // we just won't do anything.)
            //
            // This also notifies the client of the result.

            //ToDo: Commit upto the opnum
            self.commit_up_to(msg.opnum);


            //ToDo: What checks are here?
            // if msgs.len() >= self.config.quorum_size() as usize {
            //     println!("Unexpected operation: Quorum size {}, Message Length {}", 
            //         self.config.quorum_size(), 
            //         msgs.len()
            //     );
            //     return;
            // }

            // Send COMMIT message to the other replicas.
            //
            // This can be done asynchronously, so it really ought to be
            // piggybacked on the next PREPARE or something.
            let commit_message = CommitMessage {
                view: self.view,
                opnum: msg.opnum,
            };

            let serialized_commit = wrap_and_serialize(
                "CommitMessage", 
                serde_json::to_string(&commit_message).unwrap()
            );

            let _ = self.transport.broadcast(
                &self.config,
                &mut serialized_commit.as_bytes(),
            );

            //self.null_commit_timeout.reset();

            if self.last_batch_end == msg.opnum {
                self.batch_complete = true;
                if self.last_op > self.last_batch_end {
                    println!("Closing batch");
                    //self.close_batch();
                }
            }
        }

    }

    fn commit_up_to(&mut self, upto: u64) {

        println!("Committing up to {}", upto);

        while self.last_committed < upto {

            println!("Last committed: {}, Target: {}", self.last_committed, upto);

            self.last_committed += 1;

            let entry = self.log.find(self.last_committed);
            if entry.is_none() {
                panic!("Could not find entry {}", self.last_committed);
                //continue;
            }

            //Execute here | Upcall to the service: ToDo

            self.log.set_status(self.last_committed, LogEntryState::Committed);

            // Store the log in the client table

            // let entry = entry.unwrap();
            // let mut reply = ReplyMessage::default();
        }
    }

    fn handle_commit(&mut self, msg:CommitMessage) {
        println!("Received COMMIT <{}, {}>)", 
            msg.view, 
            msg.opnum,
        );

        if self.status != VR_STATUS_NORMAL {
            println!("Rejecting COMMIT while not in normal state");
            return;
        }

        if msg.view < self.view {
            println!("Rejecting COMMIT with stale view");
            return;

        }

        if msg.view > self.view {
            println!("Request for State Transfer: Leader view {}, Replica view {}", msg.view, self.view);
            //self.request_state_transfer(msg.view);
            return;
        }

        if am_leader(self.view, self.id, &self.config) {
            println!("Rejecting COMMIT - I am the leader");
            return;
        }

        //ToDo: Reset the timeout
        //self.view_change_timeout.reset();

        if msg.opnum <= self.last_committed {
            println!("Rejecting COMMIT - this was already committed");
            return;
        }

        if msg.opnum > self.last_op {
            println!("Request for State Transfer. Last op {}, Received op {}", self.last_op, msg.opnum);
            //self.request_state_transfer(msg.view);
            return;
        }

        //Proceed to handle the commit
        self.commit_up_to(msg.opnum);

    }

    fn start_view_change(&mut self, new_view: u64) {
        log::info!("Starting view change to {}", new_view);

        self.view = new_view;
        self.status = VR_STATUS_VIEW_CHANGE;

        // self.view_change_timeout.reset();
        // self.null_commit_timeout.stop();
        // self.resend_prepare_timeout.stop();
        // self.close_batch_timeout.stop();

        //let mut m = StartViewChangeMessage::default();
        // m.set_view(new_view);
        // m.set_replica(self.idx);
        // m.set_lastcommitted(self.last_committed);

        //ToDo: Send the message to all the replicas with m
    }

    fn send_null_commit(&mut self) {

        log::info!("Sending null commit");

        //let mut cm = CommitMessage::default();

        // cm.set_view(self.view);
        // cm.set_opnum(self.last_committed);

        //assert!(am_leader(self.view, idx, &config));

        //ToDo: Send the message to all the replicas with cm

    }

    pub async fn start_vr_replica(&mut self) {

        loop {
            let mut buf = [0; 1024];

            let (len , from)= self.transport.receive_from(&mut buf).expect("Didn't receive data");
            let message = String::from_utf8(buf[..len].to_vec()).expect("Failed to convert to String");

            //println!("Received message: {}", message);

            //let cloned_message = message.clone();

            let wrapper: Result<MessageWrapper, _> = serde_json::from_str(&message);

            match wrapper {
                Ok(wrapper) => {
                    match wrapper.msg_type.as_str() {
                        "RequestMessage" => {
                            let request_message: Result<RequestMessage, _> = 
                                serde_json::from_str(&wrapper.msg_content);

                            match request_message {
                                Ok(request_message) => {
                                    self.handle_request_message(request_message);
                                },
                                Err(err) => {
                                    println!("Failed to deserialize request message: {:?}", err);
                                    continue;
                                }
                            }  
                        },
                        "PrepareMessage" => {
                            let prepare_message: Result<PrepareMessage, _> = 
                                serde_json::from_str(&wrapper.msg_content);
                            
                            match prepare_message {
                                Ok(prepare_message) => {
                                    self.handle_prepare(prepare_message);
                                },
                                Err(err) => {
                                    println!("Failed to deserialize prepare message: {:?}", err);
                                    continue;
                                }
                            }
                        },

                        "PrepareOkMessage" => {
                            let prepareok_message: Result<PrepareOkMessage, _> = 
                                serde_json::from_str(&wrapper.msg_content);
                            
                            match prepareok_message {
                                Ok(prepareok_message) => {
                                    self.handle_prepareok(prepareok_message, from);
                                },
                                Err(err) => {
                                    println!("Failed to deserialize prepare_ok message: {:?}", err);
                                    continue;
                                }
                            }
                        },

                        "CommitMessage" => {
                            let commit_message: Result<CommitMessage, _> = 
                                serde_json::from_str(&wrapper.msg_content);
                            
                            match commit_message {
                                Ok(commit_message) => {
                                    self.handle_commit(commit_message);
                                },
                                Err(err) => {
                                    println!("Failed to deserialize commit message: {:?}", err);
                                    continue;
                                }
                            }
                        },

                        _ => {
                            println!("Received message of unknown type");
                        }
                    }
                }
            Err(err) => {
                println!("Failed to deserialize wrapper: {:?}", err);
                continue;
            }
            }

        }
    }
}




pub struct VsrClient {
    pub clientid:u64,
    clientreqid: u64,
    config: configuration::Config,
    socket: UdpSocket,
}

impl VsrClient {
    pub async fn new(
        clientid:u64,
        config: configuration::Config,
    ) -> Self {

        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 
        rand::thread_rng().gen_range(32010..32999));
        let socket = UdpSocket::bind(&socket_addr)
        .await
        .unwrap_or_else(|_| panic!("Could not bind client socket to {}", socket_addr));

        println!("Listening on {}", socket_addr);

        VsrClient { 
            clientid,
            clientreqid: 0,
            config,
            socket,
        }
    }

    pub async fn send_request(&mut self, op: Vec<u8>) {
        self.clientreqid += 1;
        let request = Request {
            op,
            clientid: self.clientid,
            clientreqid: self.clientreqid,
        };

        let request_message = RequestMessage {
            request,
        };

        let serialized_request = wrap_and_serialize(
            "RequestMessage", 
            serde_json::to_string(&request_message).unwrap()
        );

        // let serialized_message = serde_json::to_string(&request_message).unwrap();
        // let meta = "RequestMessage";

        // // let serialized_meta = format!("{}:{}", meta, serialized_message);

        // let wrapper = MessageWrapper {
        //     msg_type: meta.to_string(),
        //     msg_content: serialized_message,
        // };

        // let serialized_meta = serde_json::to_string(&wrapper).unwrap();

        let replica = configuration::ReplicaAddress::to_socket_addr(&self.config.replicas[0]);

        println!("Sending request to {:?}", replica);

        self.socket
            .send_to(
                serialized_request.as_bytes(),
                replica,
            )
            .await
            .expect("Failed to send request");
    }

    pub fn receive_message(
        &mut self,
        remote: &SocketAddr,
        msg_type: String,
        data: String,
    ){
        println!("Received message from {}: {}", remote, data);
    }

    pub async fn start_vr_client(&mut self, requests: u32) -> Result<(), Box<dyn std::error::Error>> {

            for _ in 0..requests {
                sleep(Duration::from_millis(10)).await;
                // Generate a random request
                let operation = match rand::thread_rng().gen_range(0..1) {
                    0 => "set",
                    //1 => "delete",
                    //2 => "append",
                    //4 => "get",
                    _ => panic!("Invalid operation"),
                };
    
                let key = generate_random_key();
                let value = rand::thread_rng().gen_range(1..1000).to_string();
    
                println!("Client: Sending request {} {} {}", operation, key, value);

                match operation {
                    "set" => {
                        self.send_request(format!("set {} {}", key, value).as_bytes().to_vec()).await;
                    },
                    _ => {
                        println!("Unknown operation: {}", operation);
                        continue; // Skip processing if unknown message type
                    }
                }

            }
            Ok(())
    }

}


fn generate_nonce() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen()
}

fn am_leader(view: u64, idx: usize, config: &Config) -> bool {
    config.get_leader(view.try_into().unwrap())  == idx
}

fn generate_random_key() -> String {
    let prefix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();

    let random_key: u32 = rand::thread_rng().gen_range(1..1000);

    format!("{}-{}", prefix, random_key)
}