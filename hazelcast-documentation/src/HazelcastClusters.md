
# Hazelcast Clusters

This chapter describes Hazelcast clusters and the ways cluster members use to form a Hazelcast cluster. 

## Discovering Cluster Members

A Hazelcast cluster is a network of cluster members that run Hazelcast. Cluster members (also called nodes) automatically join together to form a cluster. This automatic joining takes place with various discovery mechanisms that the cluster members use to find each other. Hazelcast uses the following discovery mechanisms.

- [Multicast](#discovering-members-by-multicast)
- [TCP](#discovering-members-by-tcp)
- [EC2 Cloud](#discovering-members-by-ec2-cloud)

Each discovery mechanism is explained in the following sections.

	
![image](images/NoteSmall.jpg) ***NOTE:*** *After a cluster is formed, communication between cluster members is always via TCP/IP, regardless of the discovery mechanism used.*



