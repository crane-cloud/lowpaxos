use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::net::UdpSocket;
// use tokio::time::{sleep, Duration};
use std::net::SocketAddr;
use std::str::FromStr;
use kvstore::KeyValueStore;
pub struct PaxosNode {
    id: u32,
    proposal_number: u32,
    accepted_proposal_number: u32,
    accepted_proposal_value: Option<String>,
    promises_received: HashMap<SocketAddr, (u32, Option<String>)>,
    peers: Vec<String>,
    //storage: Arc<RwLock<HashMap<String, String>>>,
    storage: KeyValueStore
}

impl PaxosNode {
    pub fn new(id: u32, peers: Vec<String>) -> Self {
        Self {
            id,
            proposal_number: 0,
            accepted_proposal_number: 0,
            accepted_proposal_value: None,
            promises_received: HashMap::new(),
            //acceptors: vec![],
            //learners: vec![],
            peers,
            storage: KeyValueStore::new(),
        }
    }

    pub fn propose(&mut self, value: String) {
        println!("Proposing value {}", value);
        self.proposal_number += 1;

        let proposal_number = self.proposal_number;
        for peer in &self.peers {
            let proposal_number = proposal_number.clone();
            let value = value.clone();

            //AM ToDO: Send a proposal to the peer

        }
    }

    pub fn handle_promise(&mut self, from: SocketAddr, proposal_number: u32, prev_proposal_number: u32, prev_value: Option<String>) {
        println!("Handling the promise from {}", from);

        if proposal_number != self.proposal_number {
            println!("Ignoring the promise from {} because the proposal number is not the same", from);
            return;
        }

        let promise = (prev_proposal_number, prev_value);
        self.promises_received.insert(from, promise);

        if self.promises_received.len() > self.peers.len() / 2 {

            println!("Received {} promises", self.promises_received.len());

            let (max_proposal_number, max_value) = self.promises_received
                .iter()
                .max_by_key(|(_, &(pn, _))| pn)
                .map(|(_, &(pn, ref pv))| (pn, pv.clone()))
                .unwrap();

            if let Some(value) = max_value {
                self.accepted_proposal_number = max_proposal_number;
                self.accepted_proposal_value = Some(value.clone());

                for peer in &self.peers {
                    let max_proposal_number = max_proposal_number.clone();
                    let value = value.clone();

                    //AM ToDO: Send an accept to the peer
                }
            }
        }
    }

    pub fn handle_accept(&mut self, from: SocketAddr, proposal_number: u32, proposal_value: Option<String>) {
        println!("Handling the accept from {}", from);

        if proposal_number >= self.accepted_proposal_number {
            self.accepted_proposal_number = proposal_number;
            self.accepted_proposal_value = proposal_value.clone();

            for peer in &self.peers {
                let proposal_number = proposal_number.clone();
                let proposal_value = proposal_value.clone();

                //AM ToDO: Send an accepted to the peer
            }
        }
    }

    pub fn handle_accepted(&mut self, from: SocketAddr, proposal_number: u32, proposal_value: String) {
        println!("Handling the accepted from {}", from);

        if proposal_number == self.proposal_number {
            // Consensus has been reached

            println!("Consensus has been reached on value {}", proposal_value);

            //self.storage.write().unwrap().
            //    insert(format!("key-{}", proposal_number), proposal_value);
            self.storage.set(format!("key-{}", proposal_number), proposal_value);            
        }

        else {
            println!("Ignoring the accepted from {} because the proposal number is not the same", from);
        }
    }

    pub async fn start(&mut self) {
        println!("Starting the Paxos node");
        println!("Peer addresses: {:?}", self.peers);

        let addr = SocketAddr::from_str(&self.peers[self.id as usize - 1]).unwrap();
        //let socket = Arc::new(UdpSocket::bind(addr));
        //let socket = UdpSocket::bind(&addr).await.unwrap();
        let socket = Arc::new(UdpSocket::bind(&addr).await.unwrap());
        
        //let socket_receive = socket.clone();
        //let socket_send = socket.clone();

        let node_id = self.id;
        let peers = self.peers.clone();

        // tokio::spawn(async move {
        //     loop {
        //         let p1 = self.peers.clone();
        //         let (from_addr, message) = receive_message(socket.clone()).await;
        //         let from_id = p1.iter().position(|p| p == &from_addr.to_string()).unwrap() as u32 + 1;

        //         let fields: Vec<&str> = message.split(":").collect();
        //         let phase = fields[0];

        //         match phase {
        //             "propose" => {
        //                 let value = fields[1];
        //                 self.propose(value.to_string());
        //             },

        //             "promise" => {
        //                 let proposal_number = fields[1].parse::<u32>().unwrap();
        //                 let prev_proposal_number = fields[2].parse::<u32>().unwrap();
        //                 let prev_value = if Some(fields[3])!= None { Some(fields[3].to_string()) } else { None };

        //                 self.handle_promise(from_id, proposal_number, prev_proposal_number, prev_value);

        //             },

        //             "accept" => {
        //                 let proposal_number = fields[1].parse::<u32>().unwrap();
        //                 let proposal_value = if Some(fields[2])!= None { Some(fields[2].to_string()) } else { None };

        //                 self.handle_accept(from_id, proposal_number, proposal_value);

        //             },

        //             "accepted" => {
        //                 let proposal_number = fields[1].parse::<u32>().unwrap();
        //                 let value = fields[2].to_string();
        //                 self.handle_accepted(from_id, proposal_number, value);
        //             },

        //             _ => panic!("Unknown phase"),
        //         }
        //     }
            
        // });

        // tokio::spawn(async move {
        //     sleep(Duration::from_secs(1)).await;
        //     let p2 = peers.clone();
        //     let p2 = p2.iter()
        //         .enumerate()
        //         .filter(|&(i, _)|i + 1 != node_id as usize).collect::<Vec<_>>();

        //     for (i, p2) in p2 {
        //         let addr = SocketAddr::from_str(p2).unwrap();
        //         send_message(addr, format!("propose:value-{}", node_id)).await;
        //     }
        // });

        // tokio::runtime::Builder::new_multi_thread()
        //     .enable_all()
        //     .build()
        //     .unwrap()
        //     .block_on(async move {
        //         loop {
        //             sleep(Duration::from_secs(1)).await;
        //             println!("Storage: {:?}", self.storage.read().unwrap());
        //         }
        //     });


    }
}
