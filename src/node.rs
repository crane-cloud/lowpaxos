use std::collections::HashMap;
use std::ops::DerefMut;
//use std::result;
use tokio::net::UdpSocket;
use std::net::SocketAddr;
use std::str::FromStr;
use kvstore::KeyValueStore;
use std::sync::Arc;
//use tokio::time::{Duration};

//use crate::send_message;
//use tokio::time::{timeout, Duration};
//use rand::Rng;
pub struct PaxosNode {
    id: u32,
    addr: UdpSocket,
    proposal_number: u32,
    accepted_proposal_number: u32,
    accepted_proposal_value: Option<String>,
    promised_proposal_number: u32,
    promised_proposal_value: Option<String>,
    promises_received: HashMap<SocketAddr, (u32, Option<String>)>,
    accepts_received: HashMap<SocketAddr, (u32, Option<String>)>,
    peers: Arc<Vec<SocketAddr>>,
    storage: KeyValueStore
}

pub struct Client {
    peers: Vec<SocketAddr>,
    socket: UdpSocket,
}

impl PaxosNode {
    pub async fn new(id: u32, addr: SocketAddr, peers: Arc<Vec<SocketAddr>>) -> Self {
        let socket = UdpSocket::bind(addr).await.unwrap();
        Self {
            id,
            addr: socket,
            proposal_number: 0,
            accepted_proposal_number: 0,
            accepted_proposal_value: None,
            promised_proposal_number: 0,
            promised_proposal_value: None,
            promises_received: HashMap::new(),
            accepts_received: HashMap::new(),
            peers,
            storage: KeyValueStore::new(),
            //commit_index
        }
    }

    //Leader sends the proposal to the peers
    pub async fn propose(&mut self, value: String) {
        self.proposal_number += 1;
        let proposal_number = self.proposal_number;

        println!("Proposing value|operation: ({}) with proposal number: {}", value, proposal_number);

        let message = format!("propose:{}:{}", proposal_number, value);
    
        for addr in self.peers.iter() {

            if addr == &self.addr.local_addr().unwrap() {
                self.promised_proposal_number = proposal_number;
                self.promised_proposal_value = Some(value.clone());
                continue;
            }

            else {
                println!("Sending proposal to {}", addr);
                let result = self.addr.send_to(message.as_bytes(), addr).await;
                match result {
                    Ok(_) => println!("Proposal sent to {}", addr),
                    Err(err) => println!("Error sending message to {}: {:?}", addr, err),
                }
            }
            // Introduce a small delay between each message sent
            //tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    //Peers handle the proposal, and promise to accept or ignore
    pub async fn handle_propose(&mut self, from: SocketAddr, proposal_number: u32, value: Option<String>) {
        println!("Handling the proposal {} from {} with value: {:?}", proposal_number, from, value);

        //Check pn is fresh, pn > accepted_pn, pn, pn > promised_pn

        if self.accepted_proposal_number > proposal_number || 
            self.promised_proposal_number > proposal_number ||
            self.proposal_number > proposal_number {

            println!("Ignoring the proposal from {} because the request is stale", from);
            return;
        }

        else { 
            self.promised_proposal_number = proposal_number;
            self.promised_proposal_value = value.clone();
            self.proposal_number = proposal_number;

            let message = format!("promise:{}:{}", proposal_number, value.unwrap_or("".to_string()));

            let propose_result = self.addr.send_to(message.as_bytes(), from).await;

            match propose_result {
                Ok(_) => println!("Promise sent to {}", from),
                Err(err) => println!("Error sending promise to {}: {:?}", from, err),
            }
        }

    }

    //Leader collects all promises from the peers and handles them
    pub async fn handle_promise(&mut self, from: SocketAddr, proposal_number: u32, promised_value: Option<String>) {
        println!("Handling the promise from {}", from);

        if proposal_number != self.proposal_number {
            println!("Ignoring the promise from {} because the request is stale", from);
            return;
        }

        else {
            let promise = (proposal_number, promised_value.clone());
            self.promises_received.insert(from, promise);
            
            if self.promises_received.len() > self.peers.len() / 2 {

                for (k, v) in self.promises_received.iter() {
                    println!("Promise from {} with value: {:?}", k, v);
                }

                println!("Received {} promises", self.promises_received.len());

                let pv = promised_value.clone();
                let message = format!("accept:{}:{}", proposal_number, pv.unwrap_or("".to_string()));

                for i in 0..self.peers.len() {
                    let addr = self.peers[i];
                    if addr == self.addr.local_addr().unwrap() {

                        //let pvr = promised_value.clone();
                        self.accepted_proposal_number = proposal_number;
                        self.accepted_proposal_value = promised_value.clone(); //ToDo

                        self.handle_operation("log", promised_value.clone().unwrap_or("".to_string()));

                        continue;
                    }

                    else {
                        println!("Sending accept to {}", addr);
                        
                        let result = self.addr.send_to(message.as_bytes(), addr).await;
                        match result {
                            Ok(_) => println!("Accept sent to {}", addr),
                            Err(err) => println!("Error sending accept to {}: {:?}", addr, err),
                        }
                    }
                }
                self.promises_received.clear();
            }
        }
    }

    //Peers handle the accept, and accept or ignore
    pub async fn handle_accept(&mut self, from: SocketAddr, proposal_number: u32, accept_value: Option<String>) {
        println!("Handling the leader accept from {} at {}", from, self.addr.local_addr().unwrap());

        if proposal_number > self.accepted_proposal_number && 
                proposal_number >= self.promised_proposal_number &&
                proposal_number >= self.proposal_number {

            self.accepted_proposal_number = proposal_number;
            self.accepted_proposal_value = accept_value.clone();


            //ToDo: the KV should now log the accepted value - at the peer
            self.handle_operation("log", accept_value.clone().unwrap_or("".to_string()));
            let message = format!("accepted:{}:{}", proposal_number, accept_value.unwrap_or("".to_string()));

            let accept_result = self.addr.send_to(message.as_bytes(), from).await;

            match accept_result {
                Ok(_) => println!("Accept {} sent to {}", self.accepted_proposal_number, from),
                Err(err) => println!("Error sending accept to {}: {:?}", from, err),
            }

        }

        else {
            println!("Ignoring the accept from {} because the request is stale", from);
            return;
        }
    }

    //Leader collects all accepts from the peers
    pub async fn handle_accepted(&mut self, from: SocketAddr, proposal_number: u32, accepted_value: Option<String>) {
        println!("Handling the peer accept from {}", from);

        if proposal_number == self.proposal_number {

            let accept = (proposal_number, accepted_value.clone());

            self.accepts_received.insert(from, accept);

            if self.accepts_received.len() > self.peers.len() / 2 {
                
                println!("Received {} accepts", self.accepts_received.len());
                
                for (k, v) in self.accepts_received.iter() {
                    println!("Accept from {} with value: {:?}", k, v);
                }                
                
                println!("Consensus has been reached on value {:?}", accepted_value);
                
                let message = format!("commit:{}:{}", proposal_number, accepted_value.clone().unwrap_or("".to_string()));
                
                for i in 0..self.peers.len() {
                    let addr = self.peers[i];
                    if addr == self.addr.local_addr().unwrap() {
                        // Commit at the leader
                        self.handle_operation("commit", accepted_value.clone().unwrap_or("".to_string()));
                        continue;
                    }
                    else {
                        println!("Sending commit to {}", addr);
                        let result = self.addr.send_to(message.as_bytes(), addr).await;
                        match result {
                            Ok(_) => println!("Commit sent to {}", addr),
                            Err(err) => println!("Error sending commit to {}: {:?}", addr, err),
                        }
                    }

                }
                self.accepts_received.clear();
            }

            else {
                println!("Not enough accepts received!");
            }           
        }

        else {
            println!("Ignoring the accepted from {} because the request is stale", from);
        }
    }

    //Peers handle the commit, and commit or ignore
    pub async fn handle_commit(&mut self, from: SocketAddr, proposal_number: u32, commit_value: String) {
        println!("Handling the commit from {}", from);

        if proposal_number == self.proposal_number {
            
            println!("Commit proposal: {} value: {}", proposal_number, commit_value);
            self.handle_operation("commit", commit_value)
        }

        else {
            println!("Ignoring the commit from {} because the request is stale", from);
        }
    }

    pub fn handle_operation(&mut self, operation_kind: &str, operation_value: String) {

        let operation_msg = operation_value.as_str().split_whitespace().collect::<Vec<&str>>();
        let operation = operation_msg[0];

        match operation_kind {
            "log" => {
                match operation {
                    "set" => {
                        let key = operation_msg[1];
                        let value = operation_msg[2];
                        self.storage.set(key.to_string(), value.to_string());
                    },
                    "get" => {
                        let key = operation_msg[1];
                        let value = self.storage.get(key);
                        println!("Value: {:?}", value);
                    },
                    "delete" => {
                        let key = operation_msg[1];
                        self.storage.delete(key);
                    },
                    "append" => {
                        let key = operation_msg[1];
                        let value = operation_msg[2];
                        self.storage.append(key, value.to_string());
                    },
                    _ => println!("Invalid operation"),
                }
        
                let log = self.storage.get_log();
                println!("Log at {} : {:?}", self.addr.local_addr().unwrap(), log);
        
            },
            "commit" => {
                self.storage.commit();
                
                let commit_log = self.storage.get_commit_log();
                println!("Commit Log at {} : {:?}", self.addr.local_addr().unwrap(), commit_log);
            },
            _ => println!("Invalid operation kind"),
        }

    }

    pub async fn start_paxos(&mut self) {
        //println!("Starting the Paxos node with id: {} and SocketAddress: {}", self.id, self.peers[self.id as usize - 1]);
        //println!("Peer addresses: {:?}", self.peers);
        println!("Paxos Node Started on {:?}", self.addr);

        //let socket = UdpSocket::bind(&self.peers[self.id as usize - 1]).await.unwrap();
        
        //tokio::spawn(async move {
            loop {
                let mut buf = [0; 1024];
                let (len, from) = self.addr.recv_from(&mut buf).await.unwrap();
                let message = String::from_utf8(buf[..len].to_vec()).unwrap();

                let fields: Vec<&str> = message.split(":").collect();
                let phase = fields[0];

                println!("Phase {}", phase);

                match phase {
                    "client" => {//ToDo: Match on different client requests
                        let value = fields[1];
                        self.propose(value.to_string()).await;
                    },
                    "propose" => {
                        let proposal_number = fields[1].parse::<u32>().unwrap();
                        let value = if Some(fields[2])!= None { Some(fields[2].to_string()) } else { None };
                        self.handle_propose(from, proposal_number, value).await;
                      },

                    "promise" => {
                        let proposal_number = fields[1].parse::<u32>().unwrap();
                        //let prev_proposal_number = fields[2].parse::<u32>().unwrap(); //in promise
                        let promised_value = if Some(fields[2])!= None { Some(fields[2].to_string()) } else { None };
                        self.handle_promise(from, proposal_number, promised_value).await;
                    },

                    "accept" => {
                        let proposal_number = fields[1].parse::<u32>().unwrap();
                        let accept_value = if Some(fields[2])!= None { Some(fields[2].to_string()) } else { None };
                        self.handle_accept(from, proposal_number, accept_value).await;
                    },

                    "accepted" => {
                        let proposal_number = fields[1].parse::<u32>().unwrap();
                        let accept_value = if Some(fields[2])!= None { Some(fields[2].to_string()) } else { None };
                        self.handle_accepted(from, proposal_number, accept_value).await;
                    },

                    "commit" => {
                        let proposal_number = fields[1].parse::<u32>().unwrap();
                        let commit_value = fields[2].to_string();
                        self.handle_commit(from, proposal_number, commit_value).await;
                    },

                    _ => {
                        println!("Unknown phase: {}", phase);
                    }
                }
            }
        //});
    }
}

impl Client {
    pub async fn new(peers: Vec<String>, client_addr: &str) -> Self {
        let socket = UdpSocket::bind(&client_addr).await.unwrap();
        println!("Listening on {}", client_addr);
        let peers: Vec<SocketAddr> = peers.iter().map(|p| SocketAddr::from_str(p).unwrap()).collect();

        Client {
            peers,
            socket,
        }
    }

    pub async fn client_request(&mut self, value: String) {
        let random_peer = rand::random::<usize>() % self.peers.len();
        let addr = self.peers[random_peer];
        let message = format!("client:{}", value);

        println!("Sending message {} to {}", message, addr);

        //let serialized_message = serde_json::to_string(&message).unwrap();

        if let Err(err) = self.socket.send_to(message.as_bytes(), addr).await {
            eprintln!("Error sending message to {}: {:?}", addr, err);
        }

        //println!("Sending message {} to {}", message, addr);
        //self.socket.send_to(message.as_bytes(), addr).await.unwrap();
    }

    pub async fn run(&mut self) {
        println!("Client initialized. Press 'p' for operation or 'q' to quit.");
        loop {
            let mut input = String::new();
            std::io::stdin().read_line(&mut input).unwrap();
            let command = input.trim();
    
            match command {
                "p" => {
                    println!("Enter the operation:");
                    let mut value = String::new();
                    std::io::stdin().read_line(&mut value).unwrap();
                    let value = value.trim().to_string();
    
                    self.client_request(value).await;
                }
                "q" => break,
                _ => println!("Invalid command. Press 'p' for operation or 'q' to quit."),
            }
        }

    }
}