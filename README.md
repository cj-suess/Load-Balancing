# Load Balancing
## Distributed Load Balancing of Computational Tasks Using Thread Pools
This program leverages thread pools in a distributed environment to alleviate computational load imbalances across a set of computational nodes. The computational task being load balanced in this assignment is similar to the proof of work computation that is performed in cryptocurrencies such as BitCoin. This is performed in a series of steps:
1. A Registry node is created to manage the network
2. n Compute nodes are created with uniquely identifying IP addresses/server ports, thread pools, and task queues
3. An overlay is created using a ring topology and applied to the registered nodes
4. The Compute nodes randomly generate tasks (between 1 and 1000) for each round specified by the Registry
5. Load balancing is performed amongst the Compute nodes themselves to distribute excess tasks from overloaded nodes to underloaded nodes
6. All marshalling/unmarshalling for Events are done manually to be sent over TCP connections
7. All TCP connections are established manually
