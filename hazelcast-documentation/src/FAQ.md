

# Frequently Asked Questions




## Why 271 as the default partition count

The partition count 271, being a prime number, is a good choice since it will be distributed to the nodes almost evenly. For a small to medium sized cluster, the count 271 gives almost even partition distribution and optimal sized partitions.  As your cluster becomes bigger, this count should be made bigger to have evenly distributed partitions.

## Is Hazelcast thread safe

Yes. All Hazelcast data structures are thread safe.

## How do nodes discover each other


When a node is started in a cluster, it will dynamically and automatically be discovered. There are three types of discovery.

-	One is the multicast. Nodes in a cluster discover each other by multicast, by default. 
-	Second is discovery by TCP/IP. The first node created in the cluster (leader) will form a list of IP addresses of other joining nodes and send this list to these nodes. So, nodes will know each other.
-	And, if your application is placed on Amazon EC2, Hazelcast has an automatic discovery mechanism, as the third discovery type. You will just give your Amazon credentials and the joining node will be discovered automatically.

Once nodes are discovered, all the communication between them will be via TCP/IP.

## What happens when a node goes down

Once a node is gone (e.g.crashes) and since data in each node has a backup in other nodes:

-	First, the backups in other nodes are restored
-	Then, data from these restored backups are recovered
-	And finally, backups for these recovered data are formed

So, eventually, no data is lost.

## How do I test the connectivity

If you notice that there is a problem about a node in joining to a cluster, you may want to perform a connectivity test between the node to be joined and a node from the cluster. The `iperf` tool can be used for this purpose. For example, you can execute the below command on one node (i.e. listening on port 5701):

`iperf -s -p 5701`

And you can execute the below command on the other node:

`iperf -c` *`<IP address>`* `-d -p 5701`

You should see the output which includes connection information such as the IP addresses, transfer speed and bandwidth. Otherwise, if the output says `No route to host`, it means a network connection problem exists.


## How do I choose keys properly

When you store a key & value in a distributed Map, Hazelcast serializes the key and value, and stores the byte array version of them in local ConcurrentHashMaps. These ConcurrentHashMaps use `equals` and `hashCode` methods of byte array version of your key. It does not take into account the actual `equals` and `hashCode` implementations of your objects. So it is important that you choose your keys in a proper way. 

Implementing `equals` and `hashCode` is not enough, it is also important that the object is always serialized into the same byte array. All primitive types like String, Long, Integer, etc. are good candidates for keys to be used in Hazelcast. An unsorted Set is an example of a very bad candidate because Java Serialization may serialize the same unsorted set in two different byte arrays.


## How do I reflect value modifications

Hazelcast always return a clone copy of a value. Modifying the returned value does not change the actual value in the map (or multimap, list, set). You should put the modified value back to make changes visible to all nodes.

```java
V value = map.get( key );
value.updateSomeProperty();
map.put( key, value );
```

Collections which return values of methods such as `IMap.keySet`, `IMap.values`, `IMap.entrySet`, `MultiMap.get`, `MultiMap.remove`, `IMap.keySet`, `IMap.values`, contain cloned values. These collections are NOT backup by related Hazelcast objects. So changes to the these are **NOT** reflected in the originals, and vice-versa.

## How do I test my Hazelcast cluster

Hazelcast allows you to create more than one instance on the same JVM. Each member is called `HazelcastInstance` and each will have its own configuration, socket and threads, i.e. you can treat them as totally separate instances. 

This enables us to write and run cluster unit tests on a single JVM. As you can use this feature for creating separate members different applications running on the same JVM (imagine running multiple web applications on the same JVM), you can also use this feature for testing Hazelcast cluster.

Let's say you want to test if two members have the same size of a map.

```java
@Test
public void testTwoMemberMapSizes() {
  // start the first member
  HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
  // get the map and put 1000 entries
  Map map1 = h1.getMap( "testmap" );
  for ( int i = 0; i < 1000; i++ ) {
    map1.put( i, "value" + i );
  }
  // check the map size
  assertEquals( 1000, map1.size() );
  // start the second member
  HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
  // get the same map from the second member
  Map map2 = h2.getMap( "testmap" );
  // check the size of map2
  assertEquals( 1000, map2.size() );
  // check the size of map1 again
  assertEquals( 1000, map1.size() );
}
```

In the test above, everything happens in the same thread. When developing multi-threaded test, coordination of the thread executions has to be carefully handled. Usage of `CountDownLatch` for thread coordination is highly recommended. You can certainly use other things. Here is an example where we need to listen for messages and make sure that we got these messages:

```java
@Test
public void testTopic() {
  // start two member cluster
  HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
  HazelcastInstance h2 = Hazelcast.newHazelcastInstance();
  String topicName = "TestMessages";
  // get a topic from the first member and add a messageListener
  ITopic<String> topic1 = h1.getTopic( topicName );
  final CountDownLatch latch1 = new CountDownLatch( 1 );
  topic1.addMessageListener( new MessageListener() {
    public void onMessage( Object msg ) {
      assertEquals( "Test1", msg );
      latch1.countDown();
    }
  });
  // get a topic from the second member and add a messageListener
  ITopic<String> topic2 = h2.getTopic(topicName);
  final CountDownLatch latch2 = new CountDownLatch( 2 );
  topic2.addMessageListener( new MessageListener() {
    public void onMessage( Object msg ) {
      assertEquals( "Test1", msg );
      latch2.countDown();
    }
  } );
  // publish the first message, both should receive this
  topic1.publish( "Test1" );
  // shutdown the first member
  h1.shutdown();
  // publish the second message, second member's topic should receive this
  topic2.publish( "Test1" );
  try {
    // assert that the first member's topic got the message
    assertTrue( latch1.await( 5, TimeUnit.SECONDS ) );
    // assert that the second members' topic got two messages
    assertTrue( latch2.await( 5, TimeUnit.SECONDS ) );
  } catch ( InterruptedException ignored ) {
  }
}
```
You can surely start Hazelcast members with different configurations. Remember to call `Hazelcast.shutdownAll()` after each test case to make sure that there is no other running member left from the previous tests.

```java
@After
public void cleanup() throws Exception {
  Hazelcast.shutdownAll();
}
```

For more information please [check our existing tests.](https://github.com/hazelcast/hazelcast/tree/master/hazelcast/src/test/java/com/hazelcast/cluster)

## How do I create separate clusters

By specifying group name and group password, you can separate your clusters in a simple way. Groupings can be by *dev*, *production*, *test*, *app*, etc.

```xml
<hazelcast>
  <group>
    <name>dev</name>
    <password>dev-pass</password>
  </group>
  ...
</hazelcast>
```

You can also set the `groupName` with programmatic configuration. JVM can host multiple Hazelcast instances. Each node can only participate in one group and it only joins to its own group, does not mess with others. Following code creates 3 separate Hazelcast nodes, `h1` belongs to `app1` cluster, while `h2` and `h3` belong to `app2` cluster.

```java
Config configApp1 = new Config();
configApp1.getGroupConfig().setName( "app1" );

Config configApp2 = new Config();
configApp2.getGroupConfig().setName( "app2" );

HazelcastInstance h1 = Hazelcast.newHazelcastInstance( configApp1 );
HazelcastInstance h2 = Hazelcast.newHazelcastInstance( configApp2 );
HazelcastInstance h3 = Hazelcast.newHazelcastInstance( configApp2 );
```

## Does Hazelcast support hundreds of nodes

Yes. Hazelcast performed a successful test on Amazon EC2 with 200 nodes.


## Does Hazelcast support thousands of clients

Yes. However, there are some points to be considered. First of all, the environment should be LAN with a high stability and the network speed should be 10 Gbps or higher. If number of nodes are high, client type should be selected as Dummy (not Smart Client). In the case of Smart Clients, since each client will open a connection to the nodes, these nodes should be powerful enough (e.g. more cores) to handle hundreds or thousands of connections and client requests. Also, using near caches in clients should be considered to lower the network traffic. And finally, the Hazelcast releases with the NIO implementation should be used (which starts with 3.2).

Also, the clients should be configured attentively. Please refer to [Java Clients](#java-client) section for configuration notes.

## What is the difference between old LiteMember and new Smart Client

LiteMember supports task execution (distributed executor service), smart client does not. Also LiteMember is highly coupled with cluster, smart client is not.

## How do you give support

Support services are divided into two: community and commercial support. Community support is provided through our [Mail Group](https://groups.google.com/forum/#!forum/hazelcast) and StackOverflow web site. For information on support subscriptions, please see [Hazelcast.com](http://hazelcast.com/support/commercial/).

## Does Hazelcast persist

No. But, Hazelcast provides `MapStore` and `MapLoader` interfaces. When you implement, for example, `MapStore` interface, Hazelcast calls your store and load methods whenever needed.

## Can I use Hazelcast in a single server

Yes. But, please note that, Hazelcast's main design focus is multi-node clusters to be used as a distribution platform. 

## How can I monitor Hazelcast

[Hazelcast Management Center](#management-center) is used to monitor and manage the nodes running Hazelcast. In addition to monitoring overall state of a cluster, data structures can be analyzed and browsed in detail, map configurations can be updated and thread dump from nodes can be taken. 

Moreover, JMX monitoring is also provided. Please see [Monitoring with JMX](#monitoring-with-jmx) section for details.

## How can I see debug level logs

By changing the log level to "Debug". Below sample lines are for **log4j** logging framework. Please see [Logging Configuration](#logging-configuration) to learn how to set logging types.

First, set the logging type as follows.

```java
String location = "log4j.configuration";
String logging = "hazelcast.logging.type";
System.setProperty( logging, "log4j" );
/**if you want to give a new location. **/
System.setProperty( location, "file:/path/mylog4j.properties" );
```

Then set the log level to "Debug" in properties file. Below is a sample content.


`# direct log messages to stdout #`

`log4j.appender.stdout=org.apache.log4j.ConsoleAppender`

`log4j.appender.stdout.Target=System.out`

`log4j.appender.stdout.layout=org.apache.log4j.PatternLayout`

`log4j.appender.stdout.layout.ConversionPattern=%d{ABSOLUTE} %5p [%c{1}] - %m%n`

<br> </br>

`log4j.logger.com.hazelcast=debug`

`#log4j.logger.com.hazelcast.cluster=debug`

`#log4j.logger.com.hazelcast.partition=debug`

`#log4j.logger.com.hazelcast.partition.InternalPartitionService=debug`

`#log4j.logger.com.hazelcast.nio=debug`

`#log4j.logger.com.hazelcast.hibernate=debug`

The line `log4j.logger.com.hazelcast=debug` is used to see debug logs for all Hazelcast operations. Below this line, you can select to see specific logs (cluster, partition, hibernate, etc.).

## What is the difference between client-server and embedded topologies

In the embedded topology, nodes include both the data and application. This type of topology is the most useful if your application focuses on high performance computing and many task executions. Since application is close to data, this topology supports data locality. 

In the client-server topology, you create a cluster of nodes and scale the cluster independently. And your applications are hosted on the clients, that communicate with the nodes in the cluster to reach data. 

Client-server topology fits better, if there are multiple applications sharing the same data or if application deployment is significantly greater than the cluster size (e.g. 500 application servers vs. 10 node cluster).

## How do I know it is safe to kill the second node

Programmatically:

```java
PartitionService partitionService = hazelcastInstance.getPartitionService().isClusterSafe()
if (partitionService().isClusterSafe()) {
  hazelcastInstance.shutdown(); // or terminate
}
```

OR 

```java
PartitionService partitionService = hazelcastInstance.getPartitionService().isClusterSafe()
if (partitionService().isLocalMemberSafe()) {
  hazelcastInstance.shutdown(); // or terminate
}
```


## When do I need Native Memory solutions

Native Memory solutions can be preferred;

- When the amount of data per node is large enough to create significant garbage collection pauses,
- When your application requires predictable latency.


## Is there any disadvantage of using near-cache

The only disadvantage when using near-cache is that it may cause stale reads.

## Is Hazelcast secure

Hazelcast supports symmetric encryption, secure sockets layer (SSL) and Java Authentication and Authorization Service (JAAS). Please see [Security](#security) chapter for more information.



## How can I set socket options

Hazelcast allows to set some socket options such as `SO_KEEPALIVE`, `SO_SNDBUF`, `SO_RCVBUF` using Hazelcast configuration properties. Please see `hazelcast.socket.*` properties explained at [Advanced Configuration Properties](#advanced-configuration-properties).

## I periodically see client disconnections during idle time

In Hazelcast, socket connections are created with `SO_KEEPALIVE` option enabled by default. In most operating systems, default keep-alive time is 2 hours. If you have a firewall between clients and servers which is configured to reset idle connections/sessions, make sure that firewall's idle timeout is greater than TCP keep-alive defined in OS.

For additional information please see:

 - [Using TCP keepalive under Linux](http://tldp.org/HOWTO/TCP-Keepalive-HOWTO/usingkeepalive.html)
 - [Microsoft TechNet](http://technet.microsoft.com/en-us/library/cc957549.aspx)

## How to get rid of "java.lang.OutOfMemoryError: unable to create new native thread"

If you encounter an error of `java.lang.OutOfMemoryError: unable to create new native thread`, it may be caused by exceeding the available file descriptors on your operating system, especially if it is Linux. This exception is usually thrown on a running node, after a period of time when thread count exhausts file descriptor availability.

The JVM on Linux consumes a file descriptor for each thread created.  The default number of file descriptors available in Linux is usually 1024, so if you have many JVMs running on a single machine it is possible to exceed this.

You can view the limit using the following command

`# ulimit -a`

At operating system level, Linux users can control the amount of resources (and in particular file descriptors) used via one of the following options.

1 - Editing the `limits.conf` file:

`# vi /etc/security/limits.conf` 

```
testuser soft nofile 4096<br>
testuser hard nofile 10240<br>
```

2 - Or, using the `ulimit` command:

`# ulimit -Hn`

```
10240
```

The default number of process per users is 1024. So adding the following to your `$HOME/.profile` could solve the issue:

`# ulimit -u 4096`

## Which virtualization should I use on AWS

AWS uses two virtualization types to launch the EC2 instances: Para-Virtualization (PV) and Hardware-assisted Virtual Machine (HVM). According to the tests we performed, HVM provided up to three times higher throughput than PV. Therefore, we recommend you to use HVM when you run Hazelcast on EC2.





