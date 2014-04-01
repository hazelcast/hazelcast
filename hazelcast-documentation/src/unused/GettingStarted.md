

## Getting Started

### Installing Hazelcast

It is more than simple to start enjoying Hazelcast:

-   Download `hazelcast-`*`<version>`*`.zip` from [www.hazelcast.org](http://www.hazelcast.org/download/).

-   Unzip `hazelcast-`*`<version>`*`.zip` file.

-   Add `hazelcast-`*`<version>`*`.jar` file into your classpath.

That is all. 

Alternatively, Hazelcast can be found in the standard Maven repositories. So, if your project uses Maven, you do not need to add additional repositories to your `pom.xml`. Just add the following lines to the `pom.xml`:

	<dependencies> 
		<dependency>			<groupId>com.hazelcast</groupId>
			<artifactId>hazelcast</artifactId>			<version>3.2</version> 
		</dependency>	</dependencies>

### Starting the Cluster and Client


Having `hazelcast-`*`<version>`*`.jar` added to you classpath, it is time to get started. 

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
        clientConfig.addAddress("127.0.0.1:5701");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap map = client.getMap("customers");
        System.out.println("Map Size:" + map.size());
    }
}
```
-   When you run it, you will see the client properly connecting to the cluster and printing the map size as **3**.

Hazelcast also offers a tool, **Management Center**, that enables monitoring your cluster. To able to use it, deploy the `mancenter-`*`<version>`*`.war` included in the ZIP file to your web server. You can use it to monitor your maps, queues, other distributed data structures and nodes. Please see [Management Center](#management-center) for usage explanations. 




## Resources


-	Hazelcast source code can be found at [Github/Hazelcast](https://github.com/hazelcast/hazelcast).
-	Hazelcast API can be found at [Hazelcast.org](http://www.hazelcast.org/docs/latest/javadoc/).
-	More use cases and resources can be found at [Hazelcast.com](http://www.hazelcast.com).
-	Questions and discussions can be post in [Hazelcast mail group](https://groups.google.com/forum/#!forum/hazelcast).



