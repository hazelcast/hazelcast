

### Starting the Cluster and Client


Having `hazelcast-`*`<version>`*`.jar` added to your classpath, it is time to get started. 

In this short tutorial, we will:

1.	Create a simple Java application using Hazelcast distributed map and queue. 
2.	Then, we will run our application twice to have two nodes (JVMs) clustered. 
3.	And, connect to our cluster from another Java application by using Hazelcast Native Java Client API.

Let`s begin.


-	Following code will start the first node and create and use `customers` map and queue.

```java
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;
import java.util.Queue;

public class GettingStarted {

    public static void main(String[] args) {
        Config cfg = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        Map<Integer, String> mapCustomers = instance.getMap("customers");
        mapCustomers.put(1, "Joe");
        mapCustomers.put(2, "Ali");
        mapCustomers.put(3, "Avi");

        System.out.println("Customer with key 1: "+ mapCustomers.get(1));
        System.out.println("Map Size:" + mapCustomers.size());

        Queue<String> queueCustomers = instance.getQueue("customers");
        queueCustomers.offer("Tom");
        queueCustomers.offer("Mary");
        queueCustomers.offer("Jane");
        System.out.println("First customer: " + queueCustomers.poll());
        System.out.println("Second customer: "+ queueCustomers.peek());
        System.out.println("Queue size: " + queueCustomers.size());
    }
}
```
-   Run this class second time to get the second node started. Have you seen they formed a cluster? You should see something like this:

```
Members [2] {
    Member [127.0.0.1:5701]
    Member [127.0.0.1:5702] this
}                              
```

-   Now, add `hazelcast-client-`*`<version>`*`.jar` to your classpath, too. This is required to be able to use a Hazelcast client. 

-   Following code will start a Hazelcast Client, connect to our two node cluster and print the size of our `customers` map.

```java    
package com.hazelcast.test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class GettingStartedClient {

    public static void main(String[] args) {
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap map = client.getMap("customers");
        System.out.println("Map Size:" + map.size());
    }
}
```
-   When you run it, you will see the client properly connecting to the cluster and printing the map size as **3**.

Hazelcast also offers a tool, **Management Center**, that enables monitoring your cluster. To be able to use it, deploy the `mancenter-`*`<version>`*`.war` included in the ZIP file to your web server. You can use it to monitor your maps, queues, other distributed data structures and nodes. Please see [Management Center](#management-center) for usage explanations.

<br> </br>

<font color="red">
***Related Information***
</font>

By default Hazelcast uses Multicast to discover other nodes to form a cluster.  If you are working with other hazelcast developers on the same network you may find yourself joining their clusters using the default settings.  Hazelcast provides a way to segregate clusters within the same network when using Multicast.  Please read the following FAQ. [How do I create separate clusters](#how-do-i-create-separate-clusters).  Alternatively if you do not wish to use the default Multicast mechanism, you can provide a fixed list of IP address that are allowed to join, information on how to configure this is provided in this document, [Configuring TCP/IP Cluster](#network-configuration)

*You can also check the video tutorials [here](http://hazelcast.org/getting-started/).*

