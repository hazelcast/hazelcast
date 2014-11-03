## Java Client

### Java Client Overview

Java client is the most full featured client. It is offered both with Hazelcast and Hazelcast Enterprise. Main idea behind the Java client is to provide the same Hazelcast functionality by proxying each operation through a Hazelcast node. 
It can be used to access and change distributed data or listen distributed events of an already established Hazelcast cluster from another Java application. 


### Java Client Dependencies

There are two dependencies that you should include in your classpath to start using Hazelcast client: `hazelcast.jar` and `hazelcast-client.jar`.

After adding these dependencies, you can start using Hazelcast client as if you are using Hazelcast API. The differences will be extracted throughout the below sections.

If you prefer to use maven, simply add below lines to your `pom.xml`.

```xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast-client</artifactId>
    <version>$LATEST_VERSION$</version>
</dependency>
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast</artifactId>
    <version>$LATEST_VERSION$</version>
</dependency>
```

### Getting Started with Client API 

First step is configuration. Java client can be configured declaratively or programmatically. We will use the programmatic approach throughout this tutorial. Please refer to [Java Client Declarative Configuration](#java-client-declarative-configuration) for details.

```java
ClientConfig clientConfig = new ClientConfig();
clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
clientConfig.getNetworkConfig().addAddress("10.90.0.1", "10.90.0.2:5702");
```

Second step is the initialization of HazelcastInstance to be connected to the cluster.

```java
HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
```

*This client interface is your gateway to access all Hazelcast distributed objects.*

Let's create a map and populate with some data;

```java

IMap<String, Customer> mapCustomers = client.getMap("customers");//creates the map proxy

mapCustomers.put("1", new Customer("Joe", "Smith")); 
mapCustomers.put("2", new Customer("Ali", "Selam")); 
mapCustomers.put("3", new Customer("Avi", "Noyan"));

```

As a final step, if you are done with your client, you can just shut it down as shown below. This will release all used resources and close connections to cluster.

```java

client.shutdown();

```

### Java Client Operation modes

Client has two operation modes because of the distributed nature of the data and cluster. 

#### Smart Client

In this mode, clients connect to each cluster node. As each data partition uses the well known consistent hashing algorithm, each client can send operation to the relevant cluster node which will increase the overall throughput and efficiency. This mode is the default.


#### Dummy Client

For some cases, the clients can be required to connect to a single node instead of each node in the cluster. Firewalls, security or some custom networking issues can be the reason for that. 
 
In this mode, client will only connect to one of the configured addresses. This single node will behave as a gateway to other nodes. For any operation requested from the client, it will redirect the request to the relevant node and return the response back to the client returned from this node.

### Fail Case Handling

There are two main failure cases to be aware of and configured to achieve a proper behavior.

#### Client Connection Failure


While client is trying to connect initially to one of the members in the `ClientNetworkConfig.addressList`, all members might be not available. Instead of giving up, throwing an exception and stopping the client, it will attempt to retry as much as `connectionAttemptLimit` times. Please see [Connection Attempt Limit](#connection-attempt-limit).

Client executes each operation through the already established connection to cluster. If this connection(s) disconnects or drops, client will try to reconnect as configured. 


#### Retry-able Operation Failure

While sending the requests to related nodes, it is possible that operation fails due to various reasons. For any read only operations, you can have your client retrying to send the operation by enabling `redoOperation`. Please see [Redo Operation](#redo-operation).

And, the number of retries is given with the property  `hazelcast.client.request.retry.count` in `ClientProperties`. It will resend the request as many as RETRY-COUNT then it will throw an exception. Please see [Client Properties](#client-properties).


### Supported Distributed Data Structures

Most of the Distributed Data Structures are supported by the client. Please check for the exceptions for the clients in other languages.

As a general rule, these data structures are configured on the server side and simply accessed through a proxy on the client side.

#### Map

You can use any [Distributed Map](#map) object with client as shown below.

```java
Imap<Integer, String> map = client.getMap(“myMap”);

map.put(1, “Ali”);
String value= map.get(1);
map.remove(1); 
```

As Locality is ambiguous for the client, `addEntryListener` and `localKeySet` are not supported. Please see [Distributed Map](#map) for more information.

#### MultiMap

A sample usage is shown below.


```java
MultiMap<Integer, String> multiMap = client.getMultiMap("myMultiMap");

multiMap.put(1,”ali”);
multiMap.put(1,”veli”);

Collection<String> values = multiMap.get(1);
```

As Locality is ambiguous for the client, `addEntryListener`, `localKeySet` and  `getLocalMultiMapStats` are not supported. Please see [Distributed MultiMap](#multimap) for more information.

#### Queue

A sample usage is shown below.


```java
IQueue<String> myQueue = client.getQueue(“theQueue”);
myQueue.offer(“ali”)
```

`getLocalQueueStats` is not supported as locality is ambiguous for the client. Please see [Distributed Queue](#queue) for more information.

#### Topic

`getLocalTopicStats` is not supported as locality is ambiguous for the client.

#### Other Supported Distributed Structures

Below distributed data structures are also supported by the client. Since their logic is the same in both the node side and client side, you can refer to their sections as listed below.

- [Replicated Map](#replicated-map-beta)
- [MapReduce](#mapreduce)
- [List](#list)
- [Set](#set)
- [IAtomicLong](#iatomiclong)
- [IAtomicReference](#iatomicreference)
- [ICountDownLatch](#icountdownlatch)
- [ISemaphore](#isemaphore)
- [IdGenerator](#idgenerator)
- [Lock](#lock)



### Client Services

Below services are provided for some common functionalities on the client side.

#### Distributed Executor Service

This service is for distributed computing. It can be used to execute tasks on the cluster on designated partition or even on all partitions. It can also be used to process entries.  Please see [Distributed Executor Service](#executor-service) for more information.

```java
IExecutorService executorService = client.getExecutorService("default");
```


After getting an instance of `IExecutorService`, it can be used as the interface with the one provided on the server side. Please see [Distributed Computing](#distributed-computing) chapter for detailed usage.

![image](images/NoteSmall.jpg) ***NOTE:*** *This service is only supported by the Java client.*

  
#### Client Service

If you need to track clients and want to listen their connection events, see the below code.

```java
final ClientService clientService = client.getClientService();
final Collection<Client> connectedClients = clientService.getConnectedClients();

clientService.addClientListener(new ClientListener() {
    @Override
    public void clientConnected(Client client) {
	//Handle client connected event
    }

    @Override
    public void clientDisconnected(Client client) {
      //Handle client disconnected event
    }
});
```

#### Partition Service

Partition service is used to find the partition of a key. It will return all partitions. See the below code.

```java
PartitionService partitionService = client.getPartitionService();

//partition of a key
Partition partition = partitionService.getPartition(key);

//all partitions
Set<Partition> partitions = partitionService.getPartitions();
```


#### Lifecycle Service

Lifecycle handling is to;

- check the client is running,
- shutdown the client gracefully,
- terminate the client ungracefully (forced shutdown), and
- add/remove lifecycle listeners.


```java
LifecycleService lifecycleService = client.getLifecycleService();

if(lifecycleService.isRunning()){
    //it is running
}

//shutdown client gracefully        
lifecycleService.shutdown();

```

### Client Listeners

Listeners can be configured to listen to various event types on the client side. Global events not relating to any distributed object can be configured through [ListenerConfig](#listenerconfig). Whereas, distributed object listeners like map entry listeners or list item listeners should be configured through their proxies. You can refer to the related sections under each distributed data structure in this reference manual.

### Client Transactions

Transactional distributed objects are supported on the client side. Please see [Transactions](#transactions) chapter on how to use them.


### Network Configuration Options

```java
ClientConfig clientConfig = new ClientConfig();
ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
```
  
#### Address List
Address List is the initial list of cluster addresses to which the client will connect. Client uses this list to find an alive node. Although it may be enough to give only one address of a node in the cluster (since all nodes communicate with each other), it is recommended to give all nodes’ addresses.

If the port part is omitted then 5701, 5702, and 5703 will be tried in a random order.

```java
clientConfig.getNetworkConfig().addAddress("10.1.1.21", "10.1.1.22:5703");
```

You can provide multiple addresses with ports provided or not as seen above. The provided list is shuffled to try them in a random order.

Default value is *localhost*.
  
#### Smart Routing

This parameter defines whether the client is smart or a dummy one.

```java
//sets client to dummy client mode
clientConfig.getNetworkConfig().setSmartRouting(false);
```
Default is *smart client* mode.

#### Redo Operation

Enables/disables redo-able operations as described in [Retry-able Operation Failure](#retry-able-operation-failure).

```java
//enables redo 
clientConfig.getNetworkConfig().setRedoOperation(true);
```
Default is *disabled*.

#### Connection Timeout

Timeout value in milliseconds for nodes to accept client connection requests. 

```java
//enables redo 
clientConfig.getNetworkConfig().setConnectionTimeout(1000);
```

Default value is *5000* milliseconds.

#### Connection Attempt Limit

While client is trying to connect initially to one of the members in the `ClientNetworkConfig.addressList`, all might be not available. Instead of giving up, throwing exception and stopping the client, it will attempt to retry as much as `ClientNetworkConfig.connectionAttemptLimit` times.

```java
//enables redo 
clientConfig.getNetworkConfig().setConnectionAttemptLimit(5);
```

Default value is *2*.

#### Connection Attempt Period

The duration in milliseconds between connection attempts defined by `ClientNetworkConfig.connectionAttemptLimit`.

```java
//enables redo 
clientConfig.getNetworkConfig().setConnectionAttemptPeriod(5000);
```

Default value is *3000*.

#### Socket Interceptor

Client configuration to set a socket intercepter. Any class implementing `com.hazelcast.nio.SocketInterceptor` is a socket Interceptor.


```java
public interface SocketInterceptor {

    void init(Properties properties);

    void onConnect(Socket connectedSocket) throws IOException;
}
```

Socket interceptor has two steps. First, it will be initialized by the configured properties. Second, it will be informed just after socket is connected using `onConnect`.


```java
SocketInterceptorConfig socketInterceptorConfig = clientConfig
               .getNetworkConfig().getSocketInterceptorConfig();

MyClientSocketInterceptor myClientSocketInterceptor = new MyClientSocketInterceptor();

socketInterceptorConfig.setEnabled(true);
socketInterceptorConfig.setImplementation(myClientSocketInterceptor);
```

If you want to configure it with a class name instead of an instance;

```java
SocketInterceptorConfig socketInterceptorConfig = clientConfig
            .getNetworkConfig().getSocketInterceptorConfig();

MyClientSocketInterceptor myClientSocketInterceptor = new MyClientSocketInterceptor();

socketInterceptorConfig.setEnabled(true);

//These properties are provided to interceptor during init
socketInterceptorConfig.setProperty("kerberos-host","kerb-host-name");
socketInterceptorConfig.setProperty("kerberos-config-file","kerb.conf");

socketInterceptorConfig.setClassName(myClientSocketInterceptor);
```

Please see [Socket Interceptor](#socket-interceptor) section for more information.

#### Socket Options

Network socket options can be configured using `SocketOptions`. It has below methods.

- `socketOptions.setKeepAlive(x)`: Enables/disables *SO_KEEPALIVE* socket option. Default is `true`.

- `socketOptions.setTcpNoDelay(x)`: Enables/disables *TCP_NODELAY* socket option. Default is `true`.

- `socketOptions.setReuseAddress(x)`: Enables/disables *SO_REUSEADDR* socket option. Default is `true`.

- `socketOptions.setLingerSeconds(x)`: Enables/disables *SO_LINGER* with the specified linger time in seconds. Default is `3`.

- `socketOptions.setBufferSize(x)`: Sets the *SO_SNDBUF* and *SO_RCVBUF* options to the specified value in KB for this Socket. Default is `32`.


```java
SocketOptions socketOptions = clientConfig.getNetworkConfig().getSocketOptions();
socketOptions.setBufferSize(32);
socketOptions.setKeepAlive(true);
socketOptions.setTcpNoDelay(true);
socketOptions.setReuseAddress(true);
socketOptions.setLingerSeconds(3);
```

#### SSL
SSL can be used to secure the connection between client and the nodes. Please see  [SSLConfig](#sslconfig) section on how to configure it.

#### Configuration for AWS

Below sample declarative and programmatic configurations show how to configure a Java client for connecting to a Hazelcast cluster in AWS. 

Declarative Configuration:

```xml
<aws enabled="true">
  <!-- optional default value is false -->
  <inside-aws>false</inside-aws>
  <access-key>my-access-key</access-key>
  <secret-key>my-secret-key</secret-key>
  <!-- optional, default is us-east-1 -->
  <region>us-west-1</region>
  <!-- optional, default is ec2.amazonaws.com. If set, region shouldn't be set
    as it will override this property
  -->
  <host-header>ec2.amazonaws.com</host-header>
  <!-- optional -->
  <security-group-name>hazelcast-sg</security-group-name>
  <!-- optional -->
  <tag-key>type</tag-key>
  <!-- optional -->
  <tag-value>hz-nodes</tag-value>
</aws>
```

Programmatic Configuration:

```java
ClientConfig clientConfig = new ClientConfig();
ClientAwsConfig clientAwsConfig = new ClientAwsConfig();
clientAwsConfig.setInsideAws( false )
               .setAccessKey( "my-access-key" )
               .setSecretKey( "my-secret-key" )
               .setRegion( "us-west-1" )
               .setHostHeader( "ec2.amazonaws.com" )
               .setSecurityGroupName( ">hazelcast-sg" )
               .setTagKey( "type" )
               .setTagValue( "hz-nodes" );
clientConfig.getNetworkConfig().setAwsConfig( clientAwsConfig );
HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );
```

![image](images/NoteSmall.jpg) ***NOTE:*** *If *`inside-aws`* parameter is not set, private addresses of nodes will always be converted to public addresses. And, client will use public addresses to connect to nodes. In order to use private addresses, you should set it to *`true`*. Also note that, when connecting outside from AWS, setting *`inside-aws`* parameter to *`true`* will cause the client not to be able to reach to the nodes.*

#### Load Balancer

`LoadBalancer` allows you to send operations to one of a number of endpoints(Members). Its main purpose is to determine the next `Member` if queried.  It is up to the implementation to use different load balancing policies. The interface `com.hazelcast.client.LoadBalancer` should be implemented for that purpose.

If the client is configured as a smart one, only the operations that are not key based will be routed to the endpoint returned by the LoadBalancer. If it is not a smart client, `LoadBalancer` will be ignored.

For configuration see  [Load Balancer Config](#loadbalancerconfig) and [Java Client Declarative Configuration](#java-client-declarative-configuration)

```


### Client Near Cache
Hazelcast distributed map has a Near Cache feature to reduce network latencies. As the client always requests data from the cluster nodes, it can be helpful for some use cases to configure a near cache on the client side.
The client supports the exact same near cache used in Hazelcast distributed map. 

### Client SSLConfig

If SSL is desired to be enabled for the client-cluster connection, this parameter should be set. Once set, the connection (socket) is established out of an SSL factory defined either by a factory class name or factory implementation. Please see SSLConfig class in `com.hazelcast.config` package at the JavaDocs page of [Hazelcast Documentation](http://www.hazelcast.org/documentation/) web page.


### Java Client Configuration

Hazelcast Java client can be configured in two ways, declaratively or programmatically.

#### Java Client Declarative Configuration

Java client can be configured using an XML configuration file. 
Below is a generic template of a declarative configuration.

```xml

<hazelcast-client xsi:schemaLocation=
    "http://www.hazelcast.com/schema/client-config hazelcast-client-config-3.3.xsd"
                  xmlns="http://www.hazelcast.com/schema/client-config"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

    <!--Cluster name to connect-->
    <group>
        <name>GROUP_NAME</name>
        <password>GROUP_PASS</password>
    </group>

    <!--client properties-->
    <properties>
        <property name="hazelcast.client.connection.timeout">10000</property>
        <property name="hazelcast.client.retry.count">6</property>
    </properties>

    <!--Network configuration details-->
    <network>
        <cluster-members>
            <!--initial cluster members to connect-->
            <address>127.0.0.1</address>
            <address>127.0.0.2</address>
        </cluster-members>

        <smart-routing>true</smart-routing>
        <redo-operation>true</redo-operation>

        <socket-interceptor enabled="true">
            <!--socket-interceptor configuration details-->
        </socket-interceptor>

        <aws enabled="true" connection-timeout-seconds="11">
            <!--AWS configuration details-->
        </aws>
    </network>

    <!--local executor pool size-->
    <executor-pool-size>40</executor-pool-size>

    <!--security credentials configuration-->
    <security>
        <credentials>com.hazelcast.security.UsernamePasswordCredentials</credentials>
    </security>

    <listeners>
        <!--listeners-->
    </listeners>

    <serialization>
        <!--serialization configuration details-->
    </serialization>

    <proxy-factories>
        <!--ProxyFactory configuration details-->
    </proxy-factories>

    <!--load balancer configuration-->
    <!-- type can be "round-robin" or "random" -->
    <load-balancer type="random"/>

    <near-cache name="mapName">
        <!--near cache configuration details of a map-->
    </near-cache>

</hazelcast-client>
```


#### Java Client Programmatic Configuration

Using the configuration API, a `ClientConfig` configured as required. Please refer to the related sections and JavaDocs for more information.

##### ClientNetworkConfig

ClientNetworkConfig includes the below listed configuration options, which are explained under [Network Configuration Options](#network-configuration-options) section.

* addressList
* smartRouting
* redoOperation
* connectionTimeout
* connectionAttemptLimit
* connectionAttemptPeriod
* SocketInterceptorConfig
* SocketOptions
* SSLConfig
* ClientAwsConfig


##### GroupConfig
Clients should provide group name and password in order to connect to the cluster.
It can be configured using `GroupConfig`, as shown below.

```java
clientConfig.setGroupConfig(new GroupConfig("dev","dev-pass"));
```

#### LoadBalancerConfig

```java

clientConfig.setLoadBalancer(yourLoadBalancer);

```

##### ClientSecurityConfig

In the cases where the security established with `GroupConfig` is not enough and you want your clients connecting securely to the cluster, `ClientSecurityConfig` can be used. This configuration has a `credentials` parameter with which IP address and UID are set. Please see `ClientSecurityConfig.java` in our code.


##### SerializationConfig

For the client side serialization, Hazelcast configuration is used. Please refer to [Serialization](#serialization) chapter.


##### ListenerConfig
Global event listeners can be configured using `ListenerConfig` as shown below.


```java
ClientConfig clientConfig = new ClientConfig();
ListenerConfig listenerConfig = new ListenerConfig(LifecycleListenerImpl);
clientConfig.addListenerConfig(listenerConfig);
```

```java
ClientConfig clientConfig = new ClientConfig();
ListenerConfig listenerConfig = new ListenerConfig("com.hazelcast.example.MembershipListenerImpl");
clientConfig.addListenerConfig(listenerConfig);
```

There are three types of event listeners that can be added.

- LifecycleListener
- MembershipListener
- DistributedObjectListener


***RELATED INFORMATION***

*Please refer to Hazelcast JavaDocs and see LifecycleListener, MembershipListener and DistributedObjectListener in `com.hazelcast.core` package.*
<br></br>

##### NearCacheConfig
A near cache on the client side can be configured by providing a configuration per map name, as shown below.

```java
ClientConfig clientConfig = new ClientConfig();
CacheConfig nearCacheConfig = new NearCacheConfig();
nearCacheConfig.setName("mapName");
clientConfig.addNearCacheConfig(nearCacheConfig);
```

Wildcards can be used for the map name. See below samples.

```java
nearCacheConfig.setName("map*");
nearCacheConfig.setName("*map");
```

##### ClassLoader
A custom `classLoader` can be configured. It will be used by serialization service and to load any class configured in configuration like event listeners or ProxyFactories.

##### ExecutorPoolSize
Hazelcast has an internal executor service (different from the data structure *Executor Service*) that has threads and queues to perform internal operations such as handling responses. This parameter specifies the size of the pool of threads which perform these operations laying in the executor's queue. If not configured, this parameter has the value as **5 \* *core size of the client*** (i.e. it is 20 for a machine that has 4 cores).




##### Client Properties 

There are some advanced client configuration properties to tune some aspects of Hazelcast Client. These can be set as property name and value pairs through declarative configuration, programmatic configuration or JVM system property. Please see [Advanced Configuration Properties](#advanced-configuration-properties) section to learn how to set these properties.

Below table lists the client configuration properties with their descriptions.

Property Name | Default Value | Type | Description
:--------------|:---------------|:------|:------------
`hazelcast.client.heartbeat.timeout`|300000|string|Timeout for the heartbeat messages sent by the client to members. If there is no any message passing between client and member within the given time via this property in milliseconds the connection will be closed.
`hazelcast.client.heartbeat.interval`|10000|string|The frequency of heartbeat messages sent by the clients to members.
`hazelcast.client.max.failed.heartbeat.count`|3|string|When the count of failed heartbeats sent to members reaches this value, the cluster is deemed as dead by the client.
`hazelcast.client.request.retry.count`|20|string|The retry count of the connection requests by the client to the members.
`hazelcast.client.request.retry.wait.time`|250|string|The frequency of the connection retries.
`hazelcast.client.event.thread.count`|5|string|Thread count for handling incoming event packets.
`hazelcast.client.event.queue.capacity`|1000000|string|Default value of the capacity of executor that handles incoming event packets.


### Sample Codes for Client

Please refer to [Client Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/clients).


