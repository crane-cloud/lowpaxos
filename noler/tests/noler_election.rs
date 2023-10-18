// #[cfg(test)]
// mod tests {

//     use std::net::SocketAddr;

//     use noler::config::*;
//     use noler::node::*;
//     use noler::transport::Transport;
//     use log::{info, Level};

//     #[test]
//     fn test_leader_election() {

//         let monitor: SocketAddr = "127.0.0.1:3000".parse().unwrap();
//         // Create nodes and configuration
//         let config = Config::new((0,0), 5, 2,
//             vec![
//                 Replica::new(1, "127.0.0.1:3001".parse().unwrap()),
//                 Replica::new(2, "127.0.0.2:3002".parse().unwrap()),
//                 Replica::new(3, "127.0.0.3:3003".parse().unwrap()),
//                 Replica::new(4, "127.0.0.4:3004".parse().unwrap()),
//                 Replica::new(5, "127.0.0.5:3005".parse().unwrap()),
//             ],
//         );

//         let mut nodes = vec![
//             NolerReplica::new(1, "127.0.0.1:3001".parse().unwrap(), config.clone(), Transport::new("127.0.0.1:3001".parse().unwrap())),
//             NolerReplica::new(2, "127.0.0.2:3002".parse().unwrap(), config.clone(), Transport::new("127.0.0.2:3002".parse().unwrap())),
//             NolerReplica::new(3, "127.0.0.3:3003".parse().unwrap(), config.clone(), Transport::new("127.0.0.3:3003".parse().unwrap())),
//             NolerReplica::new(4, "127.0.0.4:3004".parse().unwrap(), config.clone(), Transport::new("127.0.0.4:3004".parse().unwrap())),
//             NolerReplica::new(5, "127.0.0.5:3005".parse().unwrap(), config.clone(), Transport::new("127.0.0.5:3005".parse().unwrap())),
//         ];

//         // Start the nodes and wait for leader election
//         nodes.iter_mut().for_each(|node| node.start_noler_replica());
//         std::thread::sleep(std::time::Duration::from_secs(5));

//         // Get the leader from the first node
//         let reference_leader = nodes.first().unwrap().leader.clone();

//         // Print the leader for debugging
//         println!("Reference Leader: {:?}", reference_leader);

//         // Assert that all nodes have the same leader as the reference leader
//         assert!(nodes.iter().all(|node| &node.leader == &reference_leader), "Not all nodes have the same leader.");
//     }    

//     #[test]
//     fn test_only_one_leader() {
//         // Create nodes and configuration as in the previous test

//         // Wait for leader election
//         std::thread::sleep(std::time::Duration::from_secs(5));

//         // Assert that exactly one leader is elected
//         //let leader_count = nodes.iter().filter(|&node| node.is_leader()).count();
//         //assert_eq!(leader_count, 1, "There should be only one leader.");
//     }
// }
