### Java Client

#### Overview

Java client is the most full featured client. It is distributed with both hazelcast and hazelcst Enterprise. Main idea behind the java client is to provide the same hazelcast functionality by proxying each operation through a hazelcast node. 
It can be used to access and change distributed data or listen distributed events of an already established hazelcast cluster from another Java application. 


#### Dependencies

There are two dependencies that you should include in your classpath to start using hazelcast client which are basicly hazelcast.jar and hazelcast-client.jar.

After adding these dependencies you can start using hazelcast client as if you are using hazelcast API.  The differences will be reviewed throughout this documentation.

If you prefer to use maven, simply add these to your pom.

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

#### Getting started with Client API 

First step is configuration. Java client can be configured using either XML or API. We will use API throughout this tutorial. Please refer  to [XML Configuration](xml-configuration) for details.

```java
ClientConfig clientConfig = new ClientConfig();
clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
clientConfig.getNetworkConfig().addAddress("10.90.0.1", "10.90.0.2:5702");
```

Second step, "HazelcastInstance should be initialized to connect to the cluster".

```java
HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
```

*This client interface is your gateway to access all Hazelcast distributed objects.*

Let's create a map and popupate with some data;

```java

IMap<String, Customer> mapCustomers = client.getMap("customers");//creates the map proxy

mapCustomers.put("1", new Customer("Joe", "Smith")); 
mapCustomers.put("2", new Customer("Ali", "Selam")); 
mapCustomers.put("3", new Customer("Avi", "Noyan"));

```

As a final step if you are done your client, you can just shut it down. This will release all used resources and close connections to cluster.

```java

client.shutdown();

```

#### Operation modes

Client has two operation modes because of the distributed nature of the data and cluster. 

##### Smart Client

In this mode, clients connect to each cluster node. As each data partition is well known using consistent hashing algorithm, each client can send operation to the relevant cluster node which will increase the overall throughput and efficiency. This mode is the default.


##### Dummy Client

For some cases, the clients can be required to connect to a single node instead of each node in the cluster. Firewalls, security or some custom networking issue can be the reason for that.  
In this mode, client will only connect to one of the configured addresses. This single node will behave as a gateway to other nodes. For any operation requested form the client, it will redirect the request to the relevant node and return the response back to client returned from this node.

#Fail Case Handling

There are two main failing cases to be aware of and configured to achieve a proper behavior.

##Client connection failure


While client is trying to connect initially to one of the members in the `ClientNetworkConfig.addressList` , all might be not available. Instead of giving up, throwing Exception and stopping client, it will attempt to retry as much as `connectionAttemptLimit` times.   

Client execute each operation through the already established connection to cluster. If this connection(s) disconnects or drops, client will try to reconnect as configured. 


##Retryable operation failure

While sending the requests to related nodes it is possible that operation fails due to various reasons. For any readonly operation, you can configure client to retry to send the operation with `redoOpertion`

`ClientProperties` `"hazelcast.client.retry.count"`. Refer to [Client Properties](client-properties) for detail. It will resend the request as many as RETRY-COUNT then it will throw an exception.


#Distributed Data Structures

Most of Distributed Data Structures are supported by client (Please check for the exceptions for other language clients).

As a general rule, these data structures are configured on the server side and just accessed through a proxy on the client side. 

##Map

You can use any [IMap](IMap) object with client as;

```java

Imap<Integer, String> map = client.getMap(“myMap”);

map.put(1, “Ali”);
String value= map.get(1);
map.remove(1); 

```

As Locality is ambiguous for client, addEntryListener , localKeySet are not supported.
Please refer to [IMap](#)

##MultiMap


```java
MultiMap<Integer, String> multiMap = client.getMultiMap("myMultiMap");

multiMap.put(1,”ali”);
multiMap.put(1,”veli”);

Collection<String> values = multiMap.get(1);

```

As Locality is ambiguous for client, addEntryListener , localKeySet, getLocalMultiMapStats are not supported.

Please refer to [MultiMap](#)


##ReplicatedMap
Same as Hazelcast List. Please refer to [ReplicatedMap](#)

##MapReduce
Same as Hazelcast List. Please refer to [MapReduce](#)

##Queue

```java
IQueue<String> myQueue = client.getQueue(“theQueue”);
myQueue.offer(“ali”)
```

`getLocalQueueStats` is not supported as locality is ambiguous for client.

##Topic

`getLocalTopicStats` is not supported as locality is ambiguous for client.

##List
Same as Hazelcast List. Please refer to [IList](#)

##Set
Same as Hazelcast Set. Please refer to [ISet](#)

##AtomicLong
Same as Hazelcast AtomicLong. Please refer to [AtomicLong](#)

##AtomicReference
Same as Hazelcast AtomicReference. Please refer to [AtomicReference](#)

##CountDownLatch
Same as Hazelcast CountDownLatch. Please refer to [CountDownLatch](#)

##Semaphore
Same as Hazelcast Semaphore. Please refer to [Semaphore](#)

##IdGenerator
Same as Hazelcast IdGenerator. Please refer to [IdGenerator](#)

##Lock
Same as Hazelcast Lock. Please refer to [Lock](#)

#Client Services
There are services provided for some common functionality on client side.

##Distributed Executor Service
This service is for distributed computing. It can be used to execute tasks on the cluster on designated partition or even on all partition. It can also be used to process entries. 
Please refer to [IExecutorService](#) for details.

```java
IExecutorService executorService = client.getExecutorService("default");

```
After getting an instance of `IExecutorService` , it can be used as the interface with the one provided on server side. Please refer to Distributed Computing chapter for detailed usage.

This service is only supported by Java client.
  
##Client Service

If you need to track clients and want to listener their connection events, 

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

##Partition Service

Partition service is used to find the partition of a key. It will return all partitions.

```java
PartitionService partitionService = client.getPartitionService();

//partition of a key
Partition partition = partitionService.getPartition(key);

//all partitions
Set<Partition> partitions = partitionService.getPartitions();

```

##Lifecycle Service

Lifecycle handling is for

*checking the client is running
*shutting down the client gracefully
*terminating the client ungracefully (forced shutdown)
*add/remove lifecycle listeners


```java
LifecycleService lifecycleService = client.getLifecycleService();

if(lifecycleService.isRunning()){
    //it is running
}

//shutdown client gracefully        
lifecycleService.shutdown();

```

#Listeners

Listeners can be configured to listen on various event types on client side. Global events not relating to any distributed object can be configured through [ListenerConfig](#). Whereas distributed object listeners like map entry listeners or list item listeners should be configured through their proxies. Please refer to [Listeners](#) for more information about listeners.

#Transactions

Transactional distributed object are supported on client side. Please refer to [Transactions] on how to use them


#Network Configuration Options

```java

ClientConfig clientConfig = new ClientConfig();
ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

```
  
##Address List
Address List is the initial list of cluster addresses to which the client will connect. Client uses this list to find an alive node. Although it may be enough to give only one address of a node in the cluster (since all nodes communicate with each other), it is recommended to give all nodes’ addresses.

If the port part is omitted then 5701, 5702, and 5703 will be tried in random order.

```java
clientConfig.getNetworkConfig().addAddress("10.1.1.21", "10.1.1.22:5703");
```

You can provide multiple addresses with ports provided or not as seen above. The provided list is shuffled to try them in random order.

Default value is “localhost”.
  
##Smart Routing

This parameter sets smart client or dummy client.

```java
//sets client to dummy client mode
clientConfig.getNetworkConfig().setSmartRouting(false);
```
Default is smart client mode.

##Redo Operation

Enables/disables redo-able operations as described in [Retryable operation failure](Retryable operation failure)

```java
//enables redo 
clientConfig.getNetworkConfig().setRedoOperation(true);
```
Default is disabled.

##Connection Timeout
Timeout value in milliseconds for nodes to accept client connection requests. Default value is 5000 milliseconds.

```java
//enables redo 
clientConfig.getNetworkConfig().setConnectionTimeout(1000);
```

Default is 60000.

##Connection Attempt Limit

While client is trying to connect initially to one of the members in the `ClientNetworkConfig.addressList`, all might be not available. Instead of giving up, throwing Exception and stopping client, it will attempt to retry as much as `ClientNetworkConfig.connectionAttemptLimit` times

```java
//enables redo 
clientConfig.getNetworkConfig().setConnectionAttemptLimit(5);
```

Default is 2.

##Connection Attempt Period

The duration in milliseconds between connection attempts defined by `ClientNetworkConfig.connectionAttemptLimit`.

```java
//enables redo 
clientConfig.getNetworkConfig().setConnectionAttemptPeriod(5000);
```

Default is 3000.

##Socket Interceptor

Client configuration to set a socket intercepter. Any class implementing `com.hazelcast.nio.SocketInterceptor` is a socket Interceptor.


```java
public interface SocketInterceptor {

    void init(Properties properties);

    void onConnect(Socket connectedSocket) throws IOException;
}
```

Socket interceptor has two steps. First it will be initialize by the configured properties. Secondly it will informed just after socket is connected using `onConnect`


```java
SocketInterceptorConfig socketInterceptorConfig = clientConfig.getNetworkConfig().getSocketInterceptorConfig();

MyClientSocketInterceptor myClientSocketInterceptor = new MyClientSocketInterceptor();

socketInterceptorConfig.setEnabled(true);
socketInterceptorConfig.setImplementation(myClientSocketInterceptor);
```

if you want to configure it with a class name instead of an instance;

```java
SocketInterceptorConfig socketInterceptorConfig = clientConfig.getNetworkConfig().getSocketInterceptorConfig();

MyClientSocketInterceptor myClientSocketInterceptor = new MyClientSocketInterceptor();

socketInterceptorConfig.setEnabled(true);

//These properties are provided to interceptor during init
socketInterceptorConfig.setProperty("kerberos-host","kerb-host-name");
socketInterceptorConfig.setProperty("kerberos-config-file","kerb.conf");

socketInterceptorConfig.setClassName(myClientSocketInterceptor);
```

For details refer to chapter [13.1 Socket Interceptors]

##Socket Options

Network socket options can be configured using `SocketOptions`.

`socketOptions.setKeepAlive(x)` : Enable/disable SO_KEEPALIVE socket option. Default is `true`.

`socketOptions.setTcpNoDelay(x)` : Enable/disable TCP_NODELAY socket option. Default is `true`.

`socketOptions.setReuseAddress(x)` : Enable/disable the SO_REUSEADDR socket option. Default is `true`.

`socketOptions.setLingerSeconds(x)` : Enable/disable SO_LINGER with the specified linger time in seconds. Default is `3`.

`socketOptions.setBufferSize(x)` : Sets the SO_SNDBUF and SO_RCVBUF options to the specified value in KB for this Socket. Default is `32`.


```java
SocketOptions socketOptions = clientConfig.getNetworkConfig().getSocketOptions();
socketOptions.setBufferSize(32);
socketOptions.setKeepAlive(true);
socketOptions.setTcpNoDelay(true);
socketOptions.setReuseAddress(true);
socketOptions.setLingerSeconds(3);
```

##SSL
SSL can be used to secure the connection between client and the nodes. Refer to [SSLConfig](sslconfig) on how to configure it.

###Configuration for AWS

Below sample XML and programmatic configurations show how to configure a Java client for connecting to a Hazelcast cluster in AWS. 

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

***Note:*** *If *`inside-aws`* parameter is not set, private addresses of nodes will always be converted to public addresses. And, client will use public addresses to connect to nodes. In order to use private adresses, you should set it to *`true`*. Also note that, when connecting outside from AWS, setting *`inside-aws`* parameter to *`true`* will cause the client not to be able to reach to the nodes.*

###Load Balancer

`LoadBalancer` allows you to send operations to one of a number of endpoints(Members). It's main purpose is to determine the next `Member` if queried.  It is up to the implementation to use different load balancing policies. The interface `com.hazelcast.client.LoadBalancer` sould be implemented for that purpose.

If Client is configured as smart client only the operations that are not key based will be router to the endpoint returned by the LoadBalancer. If it is not smart client, `LoadBalancer` will be ignored.

#Security
TODO


#Near Cache
Hazelcast distributed map has Near Cache feature to reduce network latencies. As client always requests data from the cluster nodes, it can be helpful for some use cases to configure a near cache on client side.
Client just supports the exact same near cache used in hazelcast map. 

##SSLConfig

If SSL is desired to be enabled for the client-cluster connection, this parameter should be set. Once set, the connection (socket) is established out of an SSL factory defined either by a factory class name or factory implementation (please see [SSLConfig](/docs/$VERSION$/javadoc/com/hazelcast/config/SSLConfig.html)).


#Configuration
Hazelcast client can be configured in two way.

##XML Configuration
Using an XML configuration file, it can be configured.
Here is a generic template of an xml configuration

```xml

<hazelcast-client xsi:schemaLocation="http://www.hazelcast.com/schema/client-config hazelcast-client-config-3.3.xsd"
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
    <load-balancer type="random"/>

    <near-cache name="mapName">
        <!--near cache configuration details of a map-->
    </near-cache>

</hazelcast-client>
```


##API Configuration
Using the configuration API, a ClientConfig configured as required. Please refer to the related parts and API doc for details.

###ClientNetworkConfig

* addressList
* smartRouting (default is  true)
* redoOperation
* connectionTimeout (default is 5000)
* connectionAttemptLimit (default is  2)
* connectionAttemptPeriod (default is  3000)
* SocketInterceptorConfig
* SocketOptions
* SSLConfig
* ClientAwsConfig


###GroupConfig
Clients should provide group name and password in order to connect to the cluster.
It can be configured using `GroupConfig`

```java
clientConfig.setGroupConfig(new GroupConfig("dev","dev-pass"));
```

###Client Properties 
TODO

###ClientSecurityConfig
In the cases where the security established with `GroupConfig` is not enough and you want your clients connecting securely to the cluster, `ClientSecurityConfig` can be used. This configuration has a `credentials` parameter with which IP address and UID are set (please see [ClientSecurityConfig.java](#com/hazelcast/client/config/ClientSecurityConfig.java)).


###SerializationConfig
For client side seriliazation, hazelcast configuration is used. Please refer to [Serialiazation](#) 

###LoadBalancer
TODO

###ListenerConfig
Global event listeners can be configured using ListenerConfig as


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
* LifecycleListener ( refer to [com.hazelcast.core.LifecycleListener](#) )
* MembershipListener ( refer to [com.hazelcast.core.MembershipListener](#) )
* DistributedObjectListener ( refer to [com.hazelcast.core.DistributedObjectListener](#) )


###NearCacheConfig
To configure a near cache on client side by providing a configuration per map name.

```java
ClientConfig clientConfig = new ClientConfig();
CacheConfig nearCacheConfig = new NearCacheConfig();
nearCacheConfig.setName("mapName");
clientConfig.addNearCacheConfig(nearCacheConfig);
```

Wildcards can be used for name as,

```java
nearCacheConfig.setName("map*");
nearCacheConfig.setName("*map");

```

###ClassLoader
A custom classLoader can be configured. It will be used by serialiazation service , and to load any class configured in configuration like event listeners or ProxyFactories.

###ExecutorPoolSize
Hazelcast has an internal executor service (different from the data structure *Executor Service*) that has threads and queues to perform internal operations such as handling responses. This parameter specifies the size of the pool of threads which perform these operations laying in the executor's queue. If not configured, this parameter has the value as **5 \* *core size of the client*** (i.e. it is 20 for a machine that has 4 cores).

###ProxyFactoryConfig
TODO, advance
###ManagedContext
TODO, advance

#Examples
REFER TO CODE SAMPLES


