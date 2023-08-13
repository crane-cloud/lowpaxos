use tokio::net::UdpSocket;
use std::net::SocketAddr;
use std::sync::Arc;
use crate::message::PaxosMessage;

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