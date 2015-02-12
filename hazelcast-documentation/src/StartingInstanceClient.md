

## Starting the Instance and Client


Having installed Hazelcast, you can get started. 

In this short tutorial, we:

1. Create a simple Java application using the Hazelcast distributed map and queue. 
2. Run our application twice to have a cluster with two nodes (JVMs). 
3. Connect to our cluster from another Java application by using the Hazelcast Native Java Client API.

Let's begin.


-	The following code starts the first instance (node), and creates and uses the `customers` map and queue.

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;
import java.util.Queue;

public class GettingStarted {
  public static void main( String[] args ) {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    Map<Integer, String> customers = hazelcastInstance.getMap( "customers" );
    customers.put( 1, "Joe" );
    customers.put( 2, "Ali" );
    customers.put( 3, "Avi" );

    System.out.println( "Customer with key 1: " + customers.get(1) );
    System.out.println( "Map Size:" + customers.size() );

    Queue<String> queueCustomers = hazelcastInstance.getQueue( "customers" );
    queueCustomers.offer( "Tom" );
    queueCustomers.offer( "Mary" );
    queueCustomers.offer( "Jane" );
    System.out.println( "First customer: " + queueCustomers.poll() );
    System.out.println( "Second customer: "+ queueCustomers.peek() );
    System.out.println( "Queue size: " + queueCustomers.size() );
  }
}
```

-   Run this `GettingStarted` class a second time to get the second node started. The nodes form a cluster. You should see something like the following.

```
Members [2] {
  Member [127.0.0.1:5701]
  Member [127.0.0.1:5702] this
}                              
```

-   Now, add the `hazelcast-client-`*`<version>`*`.jar` library to your classpath. This is required to use a Hazelcast client.

-   The following code starts a Hazelcast Client, connects to our two node cluster, and prints the size of the `customers` map.

```java    
package com.hazelcast.test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class GettingStartedClient {
    public static void main( String[] args ) {
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient( clientConfig );
        IMap map = client.getMap( "customers" );
        System.out.println( "Map Size:" + map.size() );
    }
}
```
-   When you run it, you see the client properly connecting to the cluster and printing the map size as **3**.

Hazelcast also offers a tool, **Management Center**, that enables you to monitor your cluster. To use it, deploy the `mancenter-`*`<version>`*`.war` included in the ZIP file to your web server. You can use it to monitor your maps, queues, and other distributed data structures and nodes. Please see the [Management Center section](#management-center) for usage explanations.


By default, Hazelcast uses Multicast to discover other nodes that can form a cluster.  If you are working with other Hazelcast developers on the same network, you may find yourself joining their clusters under the default settings.  Hazelcast provides a way to segregate clusters within the same network when using Multicast. Please see the FAQ item [How do I create separate clusters](#how-do-i-create-separate-clusters) for more information.  Alternatively, if you do not wish to use the default Multicast mechanism, you can provide a fixed list of IP addresses that are allowed to join. Please see the [Configuring TCP/IP Cluster section](#network-configuration) for more information.
<br> </br>

***RELATED INFORMATION***

*You can also check the video tutorials [here](http://hazelcast.org/getting-started/).*
<br> </br>

