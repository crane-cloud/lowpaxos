use tokio::net::UdpSocket;
use std::net::SocketAddr;
//use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::message::PaxosMessage;
use crate::node::PaxosNode;

pub async fn send_message(addr: SocketAddr, message: PaxosMessage) {

    let serialized_message = serde_json::to_string(&message).expect("Failed to serialize message");
    let socket =UdpSocket::bind("127.0.0.1:0").await.expect("Failed to bind socket");

    if let Err(err) = socket.send_to(serialized_message.as_bytes(), addr).await {
        eprintln!("Error sending message to {}: {:?}", addr, err);
    }
}

pub async fn receive_message(socket: Arc<UdpSocket>) -> (SocketAddr, String) {
    println!("Receiving a message on {}", socket.local_addr().unwrap());

    let mut buf = [0; 1024];
    let (len, addr) = match socket.recv_from(&mut buf).await {
        Ok((len, addr)) => (len, addr),
        Err(err) => {
            eprintln!("Error receiving message: {:?}", err);
            return (SocketAddr::from(([0, 0, 0, 0], 0)), "".to_string());
        }
    };
    let message = String::from_utf8_lossy(&buf[..len]).to_string();

    (addr, message)
}

pub async fn receive_and_handle_paxos_message(socket: Arc<UdpSocket>, node: Arc<RwLock<PaxosNode>>) {
    loop {
        let mut buf = [0; 1024];
        let (len, addr) = socket.recv_from(&mut buf).await.unwrap();
        let message = String::from_utf8_lossy(&buf[..len]).to_string();
        let message: PaxosMessage = serde_json::from_str(&message).unwrap();

        match message {
            PaxosMessage::Propose {value} => {
                let node = node.clone();
                tokio::task::spawn(async move {
                    let mut node = node.write().await;
                    node.propose(value);
                });
            }
            PaxosMessage::Promise {proposal_number, prev_proposal_number, prev_value} => {
                let node = node.clone();
                tokio::task::spawn(async move {
                    let mut node = node.write().await;
                    node.handle_promise(addr, proposal_number, prev_value);
                });
             }
            PaxosMessage::Accept {proposal_number, proposal_value} => {
                let node = node.clone();
                tokio::task::spawn(async move {
                    let mut node = node.write().await;
                    node.handle_accept(addr, proposal_number, proposal_value);
                });
            }
            PaxosMessage::Accepted {proposal_number, value} => {
                let node = node.clone();
                tokio::task::spawn(async move {
                    let mut node = node.write().await;
                    node.handle_accepted(addr, proposal_number, value);
                });
            }
        }
    }
}