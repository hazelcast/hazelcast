## Java Client

The Java client is the most full featured client. It is offered both with Hazelcast and Hazelcast Enterprise. 

### Java Client Overview

The main idea behind the Java client is to provide the same Hazelcast functionality by proxying each operation through a Hazelcast node.
It can access and change distributed data, and it can listen to distributed events of an already established Hazelcast cluster from another Java application.


### Java Client Dependencies

You should include two dependencies in your classpath to start using the Hazelcast client: `hazelcast.jar` and `hazelcast-client.jar`.

After adding these dependencies, you can start using the Hazelcast client as if you are using the Hazelcast API. The differences are discussed in the below sections.

If you prefer to use maven, add the following lines to your `pom.xml`.

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

The first step is configuration. You can configure the Java client declaratively or programmatically. We will use the programmatic approach throughout this tutorial. Please refer to the [Java Client Declarative Configuration section](#java-client-declarative-configuration) for details.

```java
ClientConfig clientConfig = new ClientConfig();
clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
clientConfig.getNetworkConfig().addAddress("10.90.0.1", "10.90.0.2:5702");
```

The second step is to initialize the HazelcastInstance to be connected to the cluster.

```java
HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
```

*This client interface is your gateway to access all Hazelcast distributed objects.*

Let's create a map and populate it with some data.

```java

IMap<String, Customer> mapCustomers = client.getMap("customers"); //creates the map proxy

mapCustomers.put("1", new Customer("Joe", "Smith"));
mapCustomers.put("2", new Customer("Ali", "Selam"));
mapCustomers.put("3", new Customer("Avi", "Noyan"));

```

As a final step, if you are done with your client, you can shut it down as shown below. This will release all the used resources and will close connections to the cluster.

```java

client.shutdown();

```

### Java Client Operation modes

The client has two operation modes because of the distributed nature of the data and cluster.

#### Smart Client

In smart mode, clients connect to each cluster node. Since each data partition uses the well known and consistent hashing algorithm, each client can send an operation to the relevant cluster node, which increases the overall throughput and efficiency. Smart mode is the default mode.


#### Dummy Client

For some cases, the clients can be required to connect to a single node instead of to each node in the cluster. Firewalls, security, or some custom networking issues can be the reason for these cases.

In dummy client mode, the client will only connect to one of the configured addresses. This single node will behave as a gateway to the other nodes. For any operation requested from the client, it will redirect the request to the relevant node and return the response back to the client returned from this node.

### Fail Case Handling

There are two main failure cases you should be aware of, and configurations you can perform to achieve proper behavior.

#### Client Connection Failure


While the client is trying to connect initially to one of the members in the `ClientNetworkConfig.addressList`, all the members might be not available. Instead of giving up, throwing an exception and stopping the client, the client will retry as many as `connectionAttemptLimit` times. Please see the [Connection Attempt Limit section](#connection-attempt-limit).

The client executes each operation through the already established connection to the cluster. If this connection(s) disconnects or drops, the client will try to reconnect as configured.


#### Retry-able Operation Failure

While sending the requests to related nodes, operation can fail due to various reasons. For any read-only operation, you can have your client retry sending the operation by enabling `redoOperation`. Please see the [Redo Operation section](#redo-operation).

The number of retries is given with the property `hazelcast.client.request.retry.count` in `ClientProperties`. The client will resend the request as many as RETRY-COUNT, then it will throw an exception. Please see the [Client Properties section](#client-properties).


### Supported Distributed Data Structures

Most of the Distributed Data Structures are supported by the client. Please check for the exceptions for the clients in other languages.

As a general rule, you configure these data structures on the server side and access them through a proxy on the client side.

#### Map

You can use any [Distributed Map](#map) object with the client, as shown below.

```java
Imap<Integer, String> map = client.getMap(“myMap”);

map.put(1, “Ali”);
String value= map.get(1);
map.remove(1);
```

Locality is ambiguous for the client, so `addEntryListener` and `localKeySet` are not supported. Please see the [Distributed Map section](#map) for more information.

#### MultiMap

A MultiMap usage example is shown below.


```java
MultiMap<Integer, String> multiMap = client.getMultiMap("myMultiMap");

multiMap.put(1,”ali”);
multiMap.put(1,”veli”);

Collection<String> values = multiMap.get(1);
```

`addEntryListener`, `localKeySet` and  `getLocalMultiMapStats` are not supported because locality is ambiguous for the client. Please see the [Distributed MultiMap section](#multimap) for more information.

#### Queue

A sample usage is shown below.


```java
IQueue<String> myQueue = client.getQueue(“theQueue”);
myQueue.offer(“ali”)
```

`getLocalQueueStats` is not supported because locality is ambiguous for the client. Please see the [Distributed Queue section](#queue) for more information.

#### Topic

`getLocalTopicStats` is not supported because locality is ambiguous for the client.

#### Other Supported Distributed Structures

The distributed data structures listed below are also supported by the client. Since their logic is the same in both the node side and client side, you can refer to their sections as listed below.

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

The distributed executor service is for distributed computing. It can be used to execute tasks on the cluster on a designated partition or on all the partitions. It can also be used to process entries. Please see the [Distributed Executor Service section](#executor-service) for more information.

```java
IExecutorService executorService = client.getExecutorService("default");
```


After getting an instance of `IExecutorService`, you can use the instance as the interface with the one provided on the server side. Please see the [Distributed Computing chapter](#distributed-computing) chapter for detailed usage.

![image](images/NoteSmall.jpg) ***NOTE:*** *This service is only supported by the Java client.*


#### Client Service

If you need to track clients and you want to listen to their connection events, see the example code below.

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

You use partition service to find the partition of a key. It will return all partitions. See the example code below.

```java
PartitionService partitionService = client.getPartitionService();

//partition of a key
Partition partition = partitionService.getPartition(key);

//all partitions
Set<Partition> partitions = partitionService.getPartitions();
```


#### Lifecycle Service

Lifecycle handling performs the following:

- checks to see if the client is running,
- shuts down the client gracefully,
- terminates the client ungracefully (forced shutdown), and
- adds/removes lifecycle listeners.


```java
LifecycleService lifecycleService = client.getLifecycleService();

if(lifecycleService.isRunning()){
    //it is running
}

//shutdown client gracefully
lifecycleService.shutdown();

```

### Client Listeners

You can configure listeners to listen to various event types on the client side. You can configure global events not relating to any distributed object through [ListenerConfig](#listenerconfig). You should configure distributed object listeners like map entry listeners or list item listeners through their proxies. You can refer to the related sections under each distributed data structure in this reference manual.

### Client Transactions

Transactional distributed objects are supported on the client side. Please see the [Transactions chapter](#transactions) on how to use them.


### Network Configuration Options

```java
ClientConfig clientConfig = new ClientConfig();
ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
```

#### Address List
Address List is the initial list of cluster addresses to which the client will connect. The client uses this list to find an alive node. Although it may be enough to give only one address of a node in the cluster (since all nodes communicate with each other), it is recommended that you give all the nodes’ addresses.

If the port part is omitted, then 5701, 5702, and 5703 will be tried in random order.

```java
clientConfig.getNetworkConfig().addAddress("10.1.1.21", "10.1.1.22:5703");
```

You can provide multiple addresses with ports provided or not as seen above. The provided list is shuffled and tried in random order.

Default value is *localhost*.

#### Smart Routing

`setSmartRouting` defines whether the client mode is smart or dummy.

```java
//sets client to dummy client mode
clientConfig.getNetworkConfig().setSmartRouting(false);
```
The default is *smart client* mode.

#### Redo Operation

`setRedoOperation` enables/disables redo-able operations as described in [Retry-able Operation Failure](#retry-able-operation-failure).

```java
//enables redo
clientConfig.getNetworkConfig().setRedoOperation(true);
```
Default is *disabled*.

#### Connection Timeout

`setConnectionTimeout` is the timeout value in milliseconds for nodes to accept client connection requests.

```java
//enables redo
clientConfig.getNetworkConfig().setConnectionTimeout(1000);
```

The default value is *5000* milliseconds.

#### Connection Attempt Limit

While the client is trying to connect initially to one of the members in the `ClientNetworkConfig.addressList`, all members might be not available. Instead of giving up, throwing an exception and stopping the client, the client will retry as many as `ClientNetworkConfig.connectionAttemptLimit` times.

```java
//enables redo
clientConfig.getNetworkConfig().setConnectionAttemptLimit(5);
```

Default value is *2*.

#### Connection Attempt Period

`etConnectionAttemptPeriod` is the duration in milliseconds between the connection attempts defined by `ClientNetworkConfig.connectionAttemptLimit`.

```java
//enables redo
clientConfig.getNetworkConfig().setConnectionAttemptPeriod(5000);
```

Default value is *3000*.

#### Client Socket Interceptor

![](images/enterprise-onlycopy.jpg)

Following is a client configuration to set a socket intercepter. Any class implementing `com.hazelcast.nio.SocketInterceptor` is a socket Interceptor.


```java
public interface SocketInterceptor {

    void init(Properties properties);

    void onConnect(Socket connectedSocket) throws IOException;
}
```

`SocketInterceptor` has two steps. First, it will be initialized by the configured properties. Second, it will be informed just after the socket is connected using `onConnect`.


```java
SocketInterceptorConfig socketInterceptorConfig = clientConfig
               .getNetworkConfig().getSocketInterceptorConfig();

MyClientSocketInterceptor myClientSocketInterceptor = new MyClientSocketInterceptor();

socketInterceptorConfig.setEnabled(true);
socketInterceptorConfig.setImplementation(myClientSocketInterceptor);
```

If you want to configure the socket connector with a class name instead of an instance, see the example below.

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

Please see the [Socket Interceptor section](#socket-interceptor) for more information.

#### Client Socket Options

You can configure the network socket options using `SocketOptions`. It has the following methods.

- `socketOptions.setKeepAlive(x)`: Enables/disables the *SO_KEEPALIVE* socket option. The default value is `true`.

- `socketOptions.setTcpNoDelay(x)`: Enables/disables the *TCP_NODELAY* socket option. The default value is `true`.

- `socketOptions.setReuseAddress(x)`: Enables/disables the *SO_REUSEADDR* socket option. The default value is `true`.

- `socketOptions.setLingerSeconds(x)`: Enables/disables *SO_LINGER* with the specified linger time in seconds. The default value is `3`.

- `socketOptions.setBufferSize(x)`: Sets the *SO_SNDBUF* and *SO_RCVBUF* options to the specified value in KB for this Socket. The default value is `32`.


```java
SocketOptions socketOptions = clientConfig.getNetworkConfig().getSocketOptions();
socketOptions.setBufferSize(32);
socketOptions.setKeepAlive(true);
socketOptions.setTcpNoDelay(true);
socketOptions.setReuseAddress(true);
socketOptions.setLingerSeconds(3);
```

#### Client SSL

![](images/enterprise-onlycopy.jpg)


You can use SSL to secure the connection between the client and the nodes. Please see the [Client SSLConfig section](#client-sslconfig) on how to configure it.

#### Configuration for AWS

The example declarative and programmatic configurations below show how to configure a Java client for connecting to a Hazelcast cluster in AWS.

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

![image](images/NoteSmall.jpg) ***NOTE:*** *If the *`inside-aws`* parameter is not set, the private addresses of nodes will always be converted to public addresses. Also, the client will use public addresses to connect to the nodes. In order to use private addresses, set the *`inside-aws`* parameter to *`true`*. Also note that, when connecting outside from AWS, setting the *`inside-aws`* parameter to *`true`* will cause the client to not be able to reach the nodes.*

#### Load Balancer

`LoadBalancer` allows you to send operations to one of a number of endpoints (Members). Its main purpose is to determine the next `Member` if queried.  It is up to your implementation to use different load balancing policies. You should implement the interface `com.hazelcast.client.LoadBalancer` for that purpose.

If the client is configured in smart mode, only the operations that are not key-based will be routed to the endpoint that is returned by the `LoadBalancer`. If the client is not a smart client, `LoadBalancer` will be ignored.

To configure client load balance, please see the [Load Balancer Config section](#loadbalancerconfig) and [Java Client Declarative Configuration section](#java-client-declarative-configuration).


### Client Near Cache
Hazelcast distributed map has a Near Cache feature to reduce network latencies. Since the client always requests data from the cluster nodes, it can be helpful for some of your use cases to configure a near cache on the client side.
The client supports the same Near Cache that is used in Hazelcast distributed map.

### Client SSLConfig

![](images/enterprise-onlycopy.jpg)

If you want SSL enabled for the client-cluster connection, you should set `SSLConfig`. Once set, the connection (socket) is established out of an SSL factory defined either by a factory class name or factory implementation. Please see the `SSLConfig` class in the `com.hazelcast.config` package at the JavaDocs page of the [Hazelcast Documentation](http://www.hazelcast.org/documentation/) web page.


### Java Client Configuration

You can declare the Hazelcast Java client declaratively or programmatically.

#### Java Client Declarative Configuration

You can configure the Java client using an XML configuration file.
Below is a generic template for a declarative configuration.

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

Using the configuration API, you can configure a `ClientConfig` as required. Please refer to the related sections and JavaDocs for more information.

##### ClientNetworkConfig

`ClientNetworkConfig` includes the configuration options listed below, which are explained in the [Network Configuration Options section](#network-configuration-options).

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
Clients should provide a group name and password in order to connect to the cluster.
You can configure them using `GroupConfig`, as shown below.

```java
clientConfig.setGroupConfig(new GroupConfig("dev","dev-pass"));
```

##### LoadBalancerConfig
The following code example shows the programmatic configuration of your load balancer.

```java
clientConfig.setLoadBalancer(yourLoadBalancer);
```

##### ClientSecurityConfig

In the cases where the security established with `GroupConfig` is not enough and you want your clients connecting securely to the cluster, you can use `ClientSecurityConfig`. This configuration has a `credentials` parameter to set the IP address and UID. Please see `ClientSecurityConfig.java` in our code.


##### SerializationConfig

For the client side serialization, use Hazelcast configuration. Please refer to the [Serialization chapter](#serialization).


##### ListenerConfig
You can configure global event listeners using `ListenerConfig` as shown below.


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

You can add three types of event listeners.

- LifecycleListener
- MembershipListener
- DistributedObjectListener


***RELATED INFORMATION***

*Please refer to Hazelcast JavaDocs and see LifecycleListener, MembershipListener and DistributedObjectListener in `com.hazelcast.core` package.*
<br></br>

##### NearCacheConfig
You can configure a Near Cache on the client side by providing a configuration per map name, as shown below.

```java
ClientConfig clientConfig = new ClientConfig();
CacheConfig nearCacheConfig = new NearCacheConfig();
nearCacheConfig.setName("mapName");
clientConfig.addNearCacheConfig(nearCacheConfig);
```

You can use wildcards can be used for the map name, as shown below.

```java
nearCacheConfig.setName("map*");
nearCacheConfig.setName("*map");
```

##### ClassLoader
You can configure a custom `classLoader`. It will be used by the serialization service and to load any class configured in configuration, such as event listeners or ProxyFactories.

##### ExecutorPoolSize
Hazelcast has an internal executor service (different from the data structure *Executor Service*) that has threads and queues to perform internal operations such as handling responses. This parameter specifies the size of the pool of threads which perform these operations laying in the executor's queue. If not configured, this parameter has the value as **5 \* *core size of the client*** (i.e. it is 20 for a machine that has 4 cores).




##### Client Properties

There are some advanced client configuration properties to tune some aspects of Hazelcast Client. You can set them as property name and value pairs through declarative configuration, programmatic configuration, or JVM system property. Please see the [Advanced Configuration Properties section](#advanced-configuration-properties) to learn how to set these properties.

The table below lists the client configuration properties with their descriptions.

Property Name | Default Value | Type | Description
:--------------|:---------------|:------|:------------
`hazelcast.client.heartbeat.timeout`|300000|string|Timeout for the heartbeat messages sent by the client to members. If no messages pass between client and member within the given time via this property in milliseconds, the connection will be closed.
`hazelcast.client.heartbeat.interval`|10000|string|The frequency of heartbeat messages sent by the clients to the members.
`hazelcast.client.max.failed.heartbeat.count`|3|string|When the count of failed heartbeats sent to the members reaches this value, the cluster is deemed as dead by the client.
`hazelcast.client.request.retry.count`|20|string|The retry count of the connection requests by the client to the members.
`hazelcast.client.request.retry.wait.time`|250|string|The frequency of the connection retries.
`hazelcast.client.event.thread.count`|5|string|The thread count for handling incoming event packets.
`hazelcast.client.event.queue.capacity`|1000000|string|The default value of the capacity of executor that handles incoming event packets.


### Sample Codes for Client

Please refer to [Client Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/clients).


