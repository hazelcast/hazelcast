
# Hazelcast Clusters

This chapter describes Hazelcast clusters and the ways cluster members use to form a Hazelcast cluster. 

## Hazelcast Cluster Discovery

A Hazelcast cluster is a network of cluster members that run Hazelcast. Cluster members, or nodes, automatically join together to form a cluster. This automatic joining takes place with various discovery mechanisms that the cluster members use to find each other. Hazelcast uses the following discovery mechanisms:

- Multicast Auto-discovery
- Discovery by TCP/IP
- EC2 Cloud Auto-discovery

Each discovery mechanism is explained in the following sections.

	
![image](images/NoteSmall.jpg) ***NOTE:*** *After a cluster is formed, communication between cluster members is always via TCP/IP, regardless of the discovery mechanism used.*



### Multicast Auto-discovery

With the multicast auto-discovery mechanism, Hazelcast allows cluster members to find each other using multicast communication. The cluster members do not need to know concrete addresses of each other, they just multicast to everyone for listening. It depends on your environment if multicast is possible or allowed.

The following is an example declarative configuration.

```xml
   <network>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
                <multicast-time-to-live>32</multicast-time-to-live>
                <multicast-timeout-seconds>2</multicast-timeout-seconds>
                <trusted-interfaces>
                   <interface>192.168.1.102</interface>
                </trusted-interfaces>   
            </multicast>
            <tcp-ip enabled="false">
           </tcp-ip>
            <aws enabled="false">
            </aws>
        </join>
   <network>     
```

You should pay attention to the `multicast-timeout-seconds` element. This element specifies the time in seconds that a node should wait for a valid multicast response from another node running in the network before declaring itself as the leader node (first node joined to the cluster) and creating its own cluster. This only applies to the startup of nodes where no leader has been assigned yet. If you specify a high value for the `multicast-timeout-seconds` like 60 seconds, it means until a leader is selected, each node is going to wait 60 seconds before moving on. Therefore, be careful when providing a high value. If the value is too low, the nodes might give up too early and create their own cluster.

<br></br>
***RELATED INFORMATION***

*Please refer to the [multicast element section](#multicast-element) for the full description of multicast discovery configuration.*
<br></br>

### Discovery by TCP/IP

If multicast is not the preferred way of discovery for your environment, then you can configure Hazelcast to be a full TCP/IP cluster. When configuring the Hazelcast for discovery by TCP/IP, you must list all or a subset of the nodes' hostnames and/or IP addresses. Note that you do not have to list all  cluster members, but at least one of them has to be active in the cluster when a new member joins.

The following is an example declarative configuration. You should set the  `enabled` attribute of `tcp-ip` element to `true`.

```xml
<hazelcast>
  ...
  <network>
    ...
    <join>
      <multicast enabled="false">
      </multicast>
      <tcp-ip enabled="true">
        <member>machine1</member>
        <member>machine2</member>
        <member>machine3:5799</member>
        <member>192.168.1.0-7</member>
        <member>192.168.1.21</member>
      </tcp-ip>
      ...
    </join>
    ...
  </network>
  ...
</hazelcast>
```

As shown above, you can provide IP addresses or hostnames for `member` elements. You can also give a range of IP addresses like `192.168.1.0-7`.

Instead of providing members line by line, you have the option to use the `members` element and write comma-separated IP addresses, as shown below.

`<members>192.168.1.0-7,192.168.1.21</members>`

If you do not provide ports for the members, Hazelcast automatically tries the ports 5701, 5702, and so on.

By default, Hazelcast binds to all local network interfaces to accept incoming traffic. You can change this behavior using the system property `hazelcast.socket.bind.any`. If you set this property to `false`, Hazelcast uses the interfaces specified in the `interfaces` element (please refer to the [Specifying Network Interfaces section](#specifying-network-interfaces)). If no interfaces are provided, then it will try to resolve one interface to bind, given in the `member` elements.

<br></br>
***RELATED INFORMATION***

*Please refer to the [tcp-ip element section](#tcp-ip-element) for the full description of discovery by TCP/IP configuration.*
<br></br>

### EC2 Cloud Auto-discovery

Hazelcast supports EC2 Auto Discovery. It is useful when you do not want or cannot provide the list of possible IP addresses. To configure your cluster to use EC2 Auto Discovery, disable join over multicast and TCP/IP, enable AWS, and provide your credentials (access and secret keys). 

You need to add *hazelcast-cloud.jar* dependency to your project. Note that it is also bundled inside *hazelcast-all.jar*. The Hazelcast cloud module does not depend on any other third party modules.

The following is an example declarative configuration.


```xml
<join>
  <multicast enabled="false">
  </multicast>
  <tcp-ip enabled="false">
  </tcp-ip>
  <aws enabled="true">
    <access-key>my-access-key</access-key>
    <secret-key>my-secret-key</secret-key>
    <region>us-west-1</region>
    <host-header>ec2.amazonaws.com</host-header>
    <security-group-name>hazelcast-sg</security-group-name>
    <tag-key>type</tag-key>
    <tag-value>hz-nodes</tag-value>
  </aws>
</join>
```  

<br></br>
***RELATED INFORMATION***

*Please refer to the [aws element section](#aws-element) for the full description of EC2 auto-discovery configuration.*
<br></br>

#### Debugging

When and if needed, Hazelcast can log the events for the instances that exist in a region. To see what has happened or to trace the activities while forming the cluster, change the log level in your logging mechanism to FINEST or DEBUG. After this change, you can also see whether the instances are accepted or rejected, and the reason the instances were rejected in the generated log. Note that changing the log level to one of the mentioned levels may affect the performance of the cluster. Please see the [Logging Configuration section](#logging-configuration) for information on logging mechanisms.

<br> </br>
***RELATED INFORMATION***

*You can download the white paper *"Hazelcast on AWS: Best Practices for Deployment"* from [Hazelcast.com](http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/).*
<br> </br>






