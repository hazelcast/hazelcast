
## Deployment Types

Basically, Hazelcast can be deployed in two types: as a Peer-to-Peer cluster or Client/Server cluster.

If you have an application whose main focal point is asynchronous or high performance computing and lots of task executions, then Peer-to-Peer deployment is the most useful. In this type, nodes include both the application and data, see the below illustration.

![](images/P2PCluster.jpg)

If you do not prefer running tasks in your cluster but storing data, you can have a cluster of server nodes that can be independently created and scaled. Your clients communicate with these server nodes to reach to the data on them. See the below illustration.

![](images/CSCluster.jpg)