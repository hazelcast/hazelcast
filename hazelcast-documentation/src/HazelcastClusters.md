
# Hazelcast Clusters

This chapter describes Hazelcast clusters and the ways cluster members use to form a Hazelcast cluster. 

## Hazelcast Cluster Discovery

A Hazelcast cluster is a network of cluster members that run Hazelcast. Cluster members, or nodes, automatically join together to form a cluster. This automatic joining takes place with various discovery mechanisms that the cluster members use to find each other. Hazelcast uses the following discovery mechanisms:

- [Multicast Auto-discovery](#multicast-auto-discovery)
- [Discovery by TCP](#discovery-by-tcp)
- [EC2 Cloud Auto-discovery](#ec2-cloud-auto-discovery)

Each discovery mechanism is explained in the following sections.

	
![image](images/NoteSmall.jpg) ***NOTE:*** *After a cluster is formed, communication between cluster members is always via TCP/IP, regardless of the discovery mechanism used.*



