
# Hazelcast Clusters

This chapter describes Hazelcast clusters and the ways cluster members use to form a Hazelcast cluster. 

## Hazelcast Cluster Discovery

A Hazelcast cluster is a network of cluster members that run Hazelcast. Cluster members, or nodes, automatically join together to form a cluster. This automatic joining takes place with various discovery mechanisms that the cluster members use to find each other. Hazelcast uses multicast auto-discovery, IP/hostname lists and EC2 cloud discovery mechanisms. The following sections describe each mechanism.
	
![image](images/NoteSmall.jpg) ***NOTE:*** *After a cluster is formed, communication between cluster members is always via TCP/IP.*

### Multicast Auto-Discovery





<br></br>
***RELATED INFORMATION***

*Please refer to the *`multicast`* bulleted item in [Network Configuration section](#join) for the configuration of multicast discovery.*
<br></br>


### IP/Hostname Lists
???


<br></br>
***RELATED INFORMATION***

*Please refer to the *`tcp-ip`* bulleted item in [Network Configuration section](#join) for the configuration IP/hostname discovery.*
<br></br>

### EC2 Auto-Discovery
???


<br></br>
***RELATED INFORMATION***

*Please refer to the *`aws`* bulleted item in [Network Configuration section](#join) for the configuration EC2 auto-discovery.*
<br></br>






