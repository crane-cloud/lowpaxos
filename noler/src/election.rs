use crate::node::NolerReplica;
use crate::message::ElectionType;
use crate::role::Role;
use crate::constants::*;
use crate::message::RequestVoteMessage;
use crate::utility::wrap_and_serialize;

pub fn start_election_cycle(replica: &mut NolerReplica, election_type: ElectionType) {
    println!("{}: starting an election cycle with type {:?}", replica.id, election_type);

    assert!(replica.role != Role::Leader, "Only a non-leader can start an election cycle");
    assert!(replica.state == ELECTION, "Only a replica in election state can start an election cycle");

    if election_type == ElectionType::Offline {
        assert!(replica.role == Role::Candidate || replica.role == Role::Witness, "Only a candidate/witness can start offline election cycle");
        assert!(replica.poll_leader_timeout.active() || replica.poll_leader_timer.active()|| !replica.heartbeat_timeout.active(), 
            "Only a candidate/witness with inactive poll leader timeout can start offline election cycle");
    }

    else if election_type == ElectionType::Profile {
        assert!(replica.role == Role::Candidate || replica.role == Role::Witness, "Only candidate/witness can start profile election cycle");
        //profile diff check
    }

    else if election_type == ElectionType::Timeout {
        assert!(replica.role == Role::Candidate, "Only a candidate can start timeout election cycle");
        assert!(replica.leader_vote_timeout.active(), "Only a candidate with inactive leader vote timeout can start timeout election cycle");
    }

    else if election_type == ElectionType::Degraded {
        assert!(replica.role == Role::Member, "Only a member can start degraded election cycle");
        assert!(replica.leadership_vote_timeout.active(), "Only a member with inactive leader vote timeout can start degraded election cycle");
    }

    else if election_type == ElectionType::Normal {
        assert!(replica.role == Role::Member, "Only a member can start normal election cycle");
        assert!(replica.leader_init_timeout.active(), "Only a member with inactive leader_init_timeout can start normal election cycle");
    }

    else {
        panic!("Invalid election type");
    }


    //Stop leader-election based timeouts
    replica.leader_init_timeout.stop();
    replica.leader_vote_timeout.stop();
    replica.leadership_vote_timeout.stop();


    for _r in replica.config.replicas.iter() {
            
        if replica.id != replica.id {
            //Create a RequestVoteMessage with term + 1
            let request_vote_message = RequestVoteMessage {
                replica_id: replica.id,
                replica_address: replica.replica_address,
                replica_role: replica.role,
                ballot: (replica.ballot.0 + 1, replica.ballot.1 + 1),
                //propose_term: replica.propose_term + 1,
                replica_profile: {
                    if let Some(profile) = replica.monitor.get((replica.id - 1) as usize) { profile.get_x() }
                    else { 0 }
                },
                election_type: election_type,
            };

            println!("{}: sending a RequestVoteMessage to Replica {}:{} with profile {}",
                replica.id, 
                replica.id, 
                replica.replica_address,
                request_vote_message.replica_profile
            );

            //Serialize the RequestVoteMessage with meta type set
            let serialized_rvm = wrap_and_serialize(
                "RequestVoteMessage", 
                serde_json::to_string(&request_vote_message).unwrap()
            );

            //Send the RequestVoteMessage to all replicas
            let _ = replica.transport.send(
                &replica.replica_address,
                &mut serialized_rvm.as_bytes(),
            );
        }
    }
}