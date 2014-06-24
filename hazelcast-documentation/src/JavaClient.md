
### Java Client

You can perform almost all Hazelcast operations with Java Client. It already implements the same interface. You must include `hazelcast.jar` and `hazelcast-client.jar` into your classpath. A sample code is shown below.

```java
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import java.util.Map;
import java.util.Collection;


ClientConfig clientConfig = new ClientConfig();
clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
clientConfig.getNetworkConfig().addAddress("10.90.0.1", "10.90.0.2:5702");

HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//All cluster operations that you can do with ordinary HazelcastInstance
Map<String, Customer> mapCustomers = client.getMap("customers");
mapCustomers.put("1", new Customer("Joe", "Smith"));
mapCustomers.put("2", new Customer("Ali", "Selam"));
mapCustomers.put("3", new Customer("Avi", "Noyan"));

Collection<Customer> colCustomers = mapCustomers.values();
for (Customer customer : colCustomers) {
     // process customer
}
```

Name and Password parameters seen above can be used to create a secure connection between the client and cluster. Same parameter values should be set at the node side, so that the client will connect to those nodes that have the same `GroupConfig` credentials, forming a separate cluster.

In the cases where the security established with `GroupConfig` is not enough and you want your clients connecting securely to the cluster, `ClientSecurityConfig` can be used. This configuration has a `credentials` parameter with which IP address and UID are set (please see [ClientSecurityConfig.java](https://github.com/hazelcast/hazelcast/blob/7133b2a84b4c97cf46f2584f1f608563a94b9e5b/hazelcast-client/src/main/java/com/hazelcast/client/config/ClientSecurityConfig.java)).

#### Java Client Configuration

To configure the parameters of client-cluster connection, `ClientNetworkConfig` is used. In this class, below parameters are set:

-	`addressList`: Includes the list of addresses to which the client will connect. Client uses this list to find an alive node. Although it may be enough to give only one address of a node in the cluster (since all nodes communicate with each other), it is recommended to give all nodes’ addresses.
-	`smartRouting`: This parameter determines whether the client is smart or dummy. A dummy client connects to one node specified in `addressList` and  stays connected to that node. If that node goes down, it chooses and connects another node. In the case of a dummy client, all operations that will be performed by the client are distributed to the cluster over the connected node. A smart client, on the other hand, connects to all nodes in the cluster and for example if the client will perform a “put” operation, it finds the node that is the key owner and performs that operation on that node.
-	`redoOperation`: Client may lost its connection to a cluster due to network issues or a node being down. In this case, we cannot know whether the operations that were being performed are completed or not. This boolean parameter determines if those operations will be retried or not. Setting this parameter to *true* for idempotent operations (e.g. “put” on a map) does not give a harm. But for operations that are not idempotent (e.g. “offer” on a queue), retrying them may cause undesirable effects. 
-	`connectionTimeout`: This parameter is the timeout in milliseconds for the heartbeat messages sent by the client to the cluster. If there is no response from a node for this timeout period, client deems the connection as down and closes it.
-	`connectionAttemptLimit` and `connectionAttemptPeriod`:  Assume that the client starts to connect to the cluster whose all nodes may not be up. First parameter is the count of connection attempts by the client and the second one is the time between those attempts (in milliseconds). These two parameters should be used together (if one of them is set, other should be set, too). Furthermore, assume that the client is connected to the cluster and everything was fine, but for a reason the whole cluster goes down. Then, the client will try to re-connect to the cluster using the values defined by these two parameters. If, for example, `connectionAttemptLimit` is set as *Integer.MAX_VALUE*, it will try to re-connect forever.
-	`socketInterceptorConfig`: When a connection between the client and cluster is established (i.e. a socket is opened) and if a socket interceptor is defined, this socket is handed to the interceptor. Interceptor can use this socket, for example, to log the connection or to handshake with the cluster. There are some cases where a socket interceptor should also be defined at the cluster side, for example, in the case of client-cluster handshaking. This can be used as a security feature, since the clients that do not have interceptors will not handshake with the cluster.
-	`sslConfig`: If SSL is desired to be enabled for the client-cluster connection, this parameter should be set. Once set, the connection (socket) is established out of an SSL factory defined either by a factory class name or factory implementation (please see [SSLConfig.java](https://github.com/hazelcast/hazelcast/blob/8f4072d372b33cb451e1fbb7fbd2c2489b631342/hazelcast/src/main/java/com/hazelcast/config/SSLConfig.java)).
-	`loadBalancer`: This parameter is used to distribute operations to multiple endpoints. It is meaningful to use it when the operation in question is not a key specific one but is a cluster wide operation (e.g. calculating the size of a map, adding a listener). Default load balancer is Round Robin. The developer can write his/her own load balancer using the [LoadBalancer](https://github.com/hazelcast/hazelcast/blob/7133b2a84b4c97cf46f2584f1f608563a94b9e5b/hazelcast-client/src/main/java/com/hazelcast/client/LoadBalancer.java) interface. 
-	`executorPoolSize`: Hazelcast has an internal executor service (different from the data structure *Executor Service*) that has threads and queues to perform internal operations such as handling responses. This parameter specifies the size of the pool of threads which perform these operations laying in the executor's queue. If not configured, this parameter has the value as **5 \* *core size of the client*** (i.e. it is 20 for a machine that has 4 cores).

#### Configuration for AWS

Below sample XML and programmatic configurations show how to configure a Java client for connecting to a Hazelcast cluster in AWS. 

Declarative Configuration:

```xml
<aws enabled="true">
    <inside-aws>false</inside-aws>  <!-- optional default value is false -->
    <access-key>my-access-key</access-key>
    <secret-key>my-secret-key</secret-key>
    <region>us-west-1</region> <!-- optional, default is us-east-1 -->
    <host-header>ec2.amazonaws.com</host-header> <!-- optional, default is ec2.amazonaws.com. If set, region shouldn't be set as it will override this property -->
    <security-group-name>hazelcast-sg</security-group-name> <!-- optional -->
    <tag-key>type</tag-key> <!-- optional -->
    <tag-value>hz-nodes</tag-value> <!-- optional -->
</aws>
```

Programmatic Configuration:

```java
final ClientConfig clientConfig = new ClientConfig();
final ClientAwsConfig clientAwsConfig = new ClientAwsConfig();
clientAwsConfig.setInsideAws(false)
               .setAccessKey("my-access-key")
               .setSecretKey("my-secret-key")
               .setRegion("us-west-1")
               .setHostHeader("ec2.amazonaws.com")
               .setSecurityGroupName(">hazelcast-sg")
               .setTagKey("type")
               .setTagValue("hz-nodes");
clientConfig.getNetworkConfig().setAwsConfig(clientAwsConfig);
final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
```

<font color='red'>***Note:***</font> *If *`inside-aws`* parameter is not set, private addresses of nodes will always be converted to public addresses. And, client will use public addresses to connect to nodes. In order to use private adresses, you should set it to *`true`*. Also note that, when connecting outside from AWS, setting *`inside-aws`* parameter to *`true`* will cause the client not to be able to reach to the nodes.*
