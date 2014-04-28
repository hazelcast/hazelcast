


## Client Types

Hazelcast provides two types of clients: *Dummy* clients and *Smart* clients.

A dummy client connects to one node in the cluster and stays connected to that node. If that node goes down, it chooses and connects another node. All operations that will be performed by the dummy client are distributed to the cluster over the connected node.

A smart client connects to all nodes in the cluster and for example if the client will perform a “put” operation, it finds the node that is the key owner and performs that operation on that node.


