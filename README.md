# lowpaxos
A Paxos derivative for a resource-constrained environment. 

## Overview
Paxos is a consensus algorithm that is used to solve the consensus problem in a distributed system. The consensus problem is the problem of agreeing on one result among a group of participants. Given an example of a key value store (KVS) application, consenus involves agreement among the participants on a set of mutable operations that can be performed on the KVS state to ensure its consistency. On availability, the KVS should service client requests with the latest updates from this consistent state. The Paxos algorithm is designed to work in an asynchronous environment where participants may experience failures.

Paxos is a two-phase protocol (also a 2-phase commit) that involves a leader election phase and a consensus phase. The leader election phase is used to elect a leader among the participants. The leader is responsible for proposing a value to be agreed upon. The consensus phase is used to agree on a value proposed by the leader. 

## Motivation
A resource-constrained environment is characterized by a lack of (or limitations in) memory, processing power, and/or network bandwidth. These environments typically use the asynchronous communication model provided by the unreliable Internet. In such an environment, the Paxos algorithm is not directly applicable. This project is an attempt to create a Paxos derivative that uses properties of the resource-constrained environment to achieve consensus while providing fault-tolerance, resource efficiency, improved throughput and performance.

## Related Work
The Paxos algorithm has been studied extensively and has been the subject of many research papers. The following are some of the papers that have been studied as part of this project and shall be used for evaluation:

1. Viewstamped Replication Revisited (or MPaxos) - http://pmg.csail.mit.edu/papers/vr-revisited.pdf
VR is a long-term and multi-leader protocol that uses a quorum-based approach to achieve consensus. It is designed to work in an asynchronous environment and provides fault-tolerance. VR provides state machine replication where client operations observe and modify the state of the service. Its primary focus is on the inevitability of failures such as in crash scenarios, and ensures that state is consistent with a minimum number of available replicas.

2. EPaxos: There Is More Consensus in Egalitarian Parliaments - https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf
EPaxos is a leaderless distributed consensus algorithm based on Paxos that requires a simple majority of replicas to maintain fault-tolerance. According to the authors, the algorithm achieves optimal commit latency in the wide-area when tolerating one and two failures, under realistic conditions; uniform load balancing across all replicas (thus achieving high throughput); and graceful performance degradation when replicas are slow or crash.

The two protocols are ideal benchmarks for the following reasons:
1. Both are designed to work in an asynchronous environment provided by the unreliable Internet.
2. VR is a leader-based protocol and EPaxos is leaderless. This project will explore both approaches.
3. EPaxos has been evaluated on a wide-area network (WAN) and our setup is similar in operation.
4. Both protocols are designed to be fault-tolerant and provide consistency guarantees on the state of a service.
5. Epaxos and VR is a key-value store application as a motivating example. This project will use a similar application.

## Implementation
We implement LowPaxos in Rust and use the Tokio framework for asynchronous communication. The implementation will initially be based on Paxos and will be extended to include optimizations from VR and EPaxos. The implementation will be evaluated on a local network and on a WAN.