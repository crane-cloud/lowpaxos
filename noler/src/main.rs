use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::net::{UdpSocket, SocketAddr};
 
pub struct Timeout {
   duration: u64,
   callback: Arc<Mutex<Box<dyn Fn() + Send + Sync>>>,
   timer_id: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}
 
impl Timeout {
   pub fn new(duration: u64, callback: Box<dyn Fn() + Send + Sync>) -> Timeout {
       Timeout {
           duration,
           callback: Arc::new(Mutex::new(callback)),
           timer_id: Arc::new(Mutex::new(None)),
       }
   }
 

   pub fn set_timeout(&mut self, duration: u64) {
       assert!(!self.active());
       self.duration = duration;
   }
 
   pub fn start(&mut self) -> Result<(), String> {
       self.reset()
   }
 
   pub fn reset(&mut self) -> Result<(), String> {
       self.stop();
 
       let callback = Arc::clone(&self.callback);
       let timer_id = Arc::clone(&self.timer_id);
       let duration = self.duration;
 
       let handle = thread::spawn(move || {
           thread::sleep(Duration::from_millis(duration));
           let callback = callback.lock().unwrap();
           (*callback)();
       });
 
       *timer_id.lock().unwrap() = Some(handle);
 
       Ok(())
   }
 
   pub fn active(&self) -> bool {
       self.timer_id.lock().unwrap().is_some()
   }
 
   pub fn stop(&mut self) {
       if self.active() {
           if let Some(handle) = self.timer_id.lock().unwrap().take() {
               handle.join().unwrap();
           }
       }
   }
}

pub struct Node {
    id: u32,
    leader_init_timeout: Timeout,
    leader_vote_timeout: Timeout,
    heartbeat_timeout: Timeout,
    rx: Receiver<String>,
}

impl Node {
    pub fn new(id: u32, tx: Sender<String>, rx: Receiver<String>) -> Node {

        let tx_init = tx.clone();
        let tx_vote = tx.clone();
        let tx_heartbeat = tx.clone();

        let node = Node {
            id,
            leader_init_timeout: Timeout::new(2000, Box::new(move || {
                tx_init.send("Init".to_string()).unwrap();
            })),
            leader_vote_timeout: Timeout::new(5000, Box::new(move || {
                tx_vote.send("Vote".to_string()).unwrap();
            })),
            heartbeat_timeout: Timeout::new(8000, Box::new(move || {
                tx_heartbeat.send("HeartBeat".to_string()).unwrap();
            })),
            rx,
        };
        node
    }

    pub fn start_node(&mut self) {
        println!("Starting node {}", self.id);

        self.leader_init_timeout.start().unwrap();
        self.leader_vote_timeout.start().unwrap();
        self.heartbeat_timeout.start().unwrap();

        loop {
            // Receive messages from the channel
            if let Ok(message) = self.rx.try_recv() {
                println!("Received message: {}", message);
            }

            thread::sleep(Duration::from_millis(1000));
        }
    }
}

fn main() {
    let (tx, rx): (Sender<String>, Receiver<String>) = unbounded();
    let mut node = Node::new(1, tx, rx); // Pass the node ID here

    let handle = thread::spawn(move || {
        node.start_node();
    });

    handle.join().unwrap();
}


fn receive_and_send(socket: &UdpSocket, tx: &Sender<String>) {
    let mut buffer = [0u8; 1024]; // Adjust buffer size as needed

    loop {
        match socket.recv_from(&mut buffer) {
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