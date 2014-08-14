

## Hazelcast Configuration Wrap Up

In Hazelcast.xml, below configuration elements are available.

- group
- management-center
- network
- partition-group
- executor-service
- queue
- map
- multimap
- list
- set
- jobtracker
- semaphore
- serialization
- services



### `group` Configuration

This configuration is to create multiple Hazelcast clusters. Each cluster will have its own group and it will not interfere with other clusters. Sample configurations are shown below.

**Declarative:**

```xml
<group>
   <name>MyGroup</name>
   <password>5551234</password>
</group>
```

**Programmatic:**

```java
Config config = new Config();
config.getGroupConfig().setName( "MyGroup" ).setPassword( "5551234" );
```
   

It has below parameters.


- `name`: Name of the group to be created.
- `password`: Password of the group to be created.


### `management-center` Configuration

This configuration is used to enable/disable Hazelcast Management Center and specify a time frequency for which the tool is updated with the cluster information. Sample configurations are shown below.

**Declarative:**

```xml
<management-center enabled="true" update-interval="3">http://localhost:8080/mancenter</management-center>
```

**Programmatic:**

```java
Config config = new Config();
config.getManagementCenterConfig().setEnabled( "true" )
         .setUrl( "http://localhost:8080/mancenter" )
            .setUpdateInterval( "3" );
```
   

It has below parameters.


- `enabled`: This attribute should be set to `true` to be enable to run Management Center.
- `url`: It is the URL where Management Center will work.
- `updateInterval`: It specifies the time frequency (in seconds) for which Management Center will take information from Hazelcast cluster.



### `network` Configuration

All network related configuration is performed via `network` tag in the XML file or the class `NetworkConfig` when using programmatic configuration. Let's first give the samples for these two approaches. Then we will look at its parameters, which are a lot.

**Declarative:**


```xml
   <network>
        <port auto-increment="true" port-count="100">5701</port>
        <outbound-ports>
            <ports>0</ports>
        </outbound-ports>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
            </tcp-ip>
            <aws enabled="false">
                <access-key>my-access-key</access-key>
                <secret-key>my-secret-key</secret-key>
                <region>us-west-1</region>
                <host-header>ec2.amazonaws.com</host-header>
                <security-group-name>hazelcast-sg</security-group-name>
                <tag-key>type</tag-key>
                <tag-value>hz-nodes</tag-value>
            </aws>
        </join>
        <interfaces enabled="false">
            <interface>10.10.1.*</interface>
        </interfaces>
        <ssl enabled="false" />
        <socket-interceptor enabled="false" />
        <symmetric-encryption enabled="false">
            <algorithm>PBEWithMD5AndDES</algorithm>
            <salt>thesalt</salt>
            <password>thepass</password>
            <iteration-count>19</iteration-count>
        </symmetric-encryption>
    </network>   
```

**Programmatic:**

```java
AwsConfig config = new AwsConfig();
config.setTagKey( "5551234" );
config.setTagValue( "Node1234" )
```

It has below parameters which are briefly described in the following subsections.

- port
- outbound-ports
- join
- interfaces
- ssl
- socket-interceptor
- symmetric-encryption

#### `port`

You can specify the ports which Hazelcast will use to communicate between cluster members. Its default value is `5701`. Sample configurations are shown below.

**Declarative:**

```xml
<network>
  <port>5701</port>
</network>
```

**Programmatic:**

```java
Config config = new Config();
config.getNetworkConfig().setPort( 5900 ); 
config.getNetworkConfig().setPortAutoIncrement( false );
```

It has below attributes.

- `port-count`: By default, Hazelcast will try 100 ports to bind. Meaning that, if you set the value of port as 5701, as members are joining to the cluster, Hazelcast tries to find ports between 5701 and 5801. You can choose to change the port count in the cases like having large instances on a single machine or willing to have only a few ports to be assigned. The parameter `port-count` is used for this purpose, whose default value is 100.

   ```xml
<network>
  <port port-count="20">5781</port>
</network>
```

- `auto-increment`: According to the above example, Hazelcast will try to find free ports between 5781 and 5801. Normally, you will not need to change this value, but it will come very handy when needed. You may also want to choose to use only one port. In that case, you can disable the auto-increment feature of `port`, as shown below.

   ```xml
<network>
  <port auto-increment="false">5701</port>
</network>
```

Naturally, the parameter `port-count` is ignored when the above configuration is made.

#### `outbound ports`


By default, Hazelcast lets the system to pick up an ephemeral port during socket bind operation. But security policies/firewalls may require to restrict outbound ports to be used by Hazelcast enabled applications. To fulfill this requirement, you can configure Hazelcast to use only defined outbound ports. Sample configurations are shown below.


**Declarative:**

```xml
  <network>
    <outbound-ports>
      <!-- ports between 33000 and 35000 -->
      <ports>33000-35000</ports>
      <!-- comma separated ports -->
      <ports>37000,37001,37002,37003</ports> 
      <ports>38000,38500-38600</ports>
    </outbound-ports>
  </network>
```

**Programmatic:**

```java
...
NetworkConfig networkConfig = config.getNetworkConfig();
// ports between 35000 and 35100
networkConfig.addOutboundPortDefinition("35000-35100");
// comma separated ports
networkConfig.addOutboundPortDefinition("36001, 36002, 36003");
networkConfig.addOutboundPort(37000);
networkConfig.addOutboundPort(37001);
...
```

***Note:*** *You can use port ranges and/or comma separated ports.*

As you can see in the programmatic configuration, if you want to add only one port you use the method `addOutboundPort`. If a group of ports needs to be added, then the method `addOutboundPortDefinition` is used. 

In the declarative one, the tag `ports` can be used for both (for single and multiple port definitions).


#### `join`

This configuration parameter is used to enable the Hazelcast instances to form a cluster, i.e. to join the members. Three ways can be used to join the members: TCP/IP, multicast and AWS (EC2). Below are sample configurations.

**Declarative:**

```xml
   <network>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
            </tcp-ip>
            <aws enabled="false">
                <access-key>my-access-key</access-key>
                <secret-key>my-secret-key</secret-key>
                <region>us-west-1</region>
                <host-header>ec2.amazonaws.com</host-header>
                <security-group-name>hazelcast-sg</security-group-name>
                <tag-key>type</tag-key>
                <tag-value>hz-nodes</tag-value>
            </aws>
        </join>
   <network>     
```

**Programmatic:**

```java
Config config = new Config();
NetworkConfig network = config.getNetworkConfig();
JoinConfig join = network.getJoin();
join.getMulticastConfig().setEnabled( false );
join.getTcpIpConfig().addMember( "10.45.67.32" ).addMember( "10.45.67.100" )
            .setRequiredMember( "192.168.10.100" ).setEnabled( true );
```

It has below elements and attributes.

- `multicast` 
	- `enabled`: Specifies whether the multicast discovery is enabled or not. Values can be `true` or `false`.
	- `multicast-group`: The multicast group IP address. Specify it when you want to create clusters within the same network. Values can be between 224.0.0.0 and 239.255.255.255. Default value is 224.2.2.3
	- `multicast-port`: The multicast socket port which Hazelcast member listens to and sends discovery messages through it. Default value is 54327.
	- `multicast-time-to-live`: 
	- `multicast-timeout-seconds`:
	- `trusted-interfaces`: 
	
- `tcp-ip`
	- 	

 


#### `partition-group` Tag

This configuration is for ???. It only has the attribute `enabled`.

#### `executor-service` Tag

This configuration is for ???. It has below attributes.

- pool-size: The number of executor threads per Member for the Executor.
- queue-capacity: Capacity of the queue.
- statistics-enabled:


#### `queue` Tag

This configuration is for ???. It has below attributes.

- max-size: Value of maximum size of Queue.
- backup-count: Value of synchronous backup count.
- async-backup-count: Value of asynchronous backup count.
- empty-queue-ttl: Value of time to live to empty the Queue
- item-listeners:
- queue-store:
- statistics-enabled:

#### `map` Tag

This configuration is for ???. It has below attributes.

- in-memory-format:
- backup-count:
- async-backup-count:
- read-backup-data:
- time-to-live-seconds:
- max-idle-seconds:
- eviction-policy:
- max-size:
- eviction-percentage:
- merge-policy:
- statistics-enabled:
- map-store:
- near-cache:
- wan-replication-ref:
- indexes:
- entry-listeners:
- partition-strategy:


#### `multimap` Tag

This configuration is for ???. It has below attributes.

- backup-count:
- async-backup-count:
- statistics-enabled:
- value-collection-type:
- entry-listeners:
- partition-strategy:


#### `topic` Tag

This configuration is for ???. It has below attributes.

- statistics-enabled:
- global-ordering-enabled:
- message-listeners:


#### `list` Tag

This configuration is for ???. It has below attributes.

- backup-count:
- async-backup-count:
- statistics-enabled:
- max-size:
- item-listeners:
- statistics-enabled:

#### `set` Tag

This configuration is for ???. It has below attributes.

- backup-count:
- async-backup-count:
- statistics-enabled:
- max-size:
- item-listeners:
- statistics-enabled:




#### `jobtracker` Tag

This configuration is for ???. It has below attributes.

- max-thread-size:
- queue-size:
- retry-count:
- chunk-size:
- communicate-stats:
- topology-changed-strategy:


#### `semaphore` Tag

This configuration is for ???. It has below attributes.

- initial-permits:
- backup-count:
- async-backup-count:


#### `serialization` Tag

This configuration is for ???. It has below attributes.

- portable-version:

#### `services` Tag

This configuration is for ???. It only has the attribute `enabled`.












Below is the `hazelcast.xml` configuration file that comes with the release, located at `bin` folder.

```xml
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config hazelcast-config-3.3.xsd"
           xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <management-center enabled="false">http://localhost:8080/mancenter</management-center>    
    <network>
        <port auto-increment="true" port-count="100">5701</port>
        <outbound-ports>
            <ports>0</ports>
        </outbound-ports>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
            </tcp-ip>
            <aws enabled="false">
                <access-key>my-access-key</access-key>
                <secret-key>my-secret-key</secret-key>
                <region>us-west-1</region>
                <host-header>ec2.amazonaws.com</host-header>
                <security-group-name>hazelcast-sg</security-group-name>
                <tag-key>type</tag-key>
                <tag-value>hz-nodes</tag-value>
            </aws>
        </join>
        <interfaces enabled="false">
            <interface>10.10.1.*</interface>
        </interfaces>
        <ssl enabled="false" />
        <socket-interceptor enabled="false" />
        <symmetric-encryption enabled="false">
            <algorithm>PBEWithMD5AndDES</algorithm>
            <salt>thesalt</salt>
            <password>thepass</password>
            <iteration-count>19</iteration-count>
        </symmetric-encryption>
    </network>   
    <partition-group enabled="false"/>   
    <executor-service name="default">
        <pool-size>16</pool-size>
        <queue-capacity>0</queue-capacity>
    </executor-service>   
    <queue name="default">
        <max-size>0</max-size>
        <backup-count>1</backup-count>
        <async-backup-count>0</async-backup-count>
        <empty-queue-ttl>-1</empty-queue-ttl>
    </queue>   
    <map name="default">
        <in-memory-format>BINARY</in-memory-format>
        <backup-count>1</backup-count>
        <async-backup-count>0</async-backup-count>
        <time-to-live-seconds>0</time-to-live-seconds>
        <max-idle-seconds>0</max-idle-seconds>
        <eviction-policy>NONE</eviction-policy>
        <max-size policy="PER_NODE">0</max-size>
        <eviction-percentage>25</eviction-percentage>
        <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
    </map>
    <multimap name="default">
        <backup-count>1</backup-count>
        <value-collection-type>SET</value-collection-type>
    </multimap>
    <multimap name="default">
        <backup-count>1</backup-count>
        <value-collection-type>SET</value-collection-type>
    </multimap>
    <list name="default">
        <backup-count>1</backup-count>
    </list>
    <set name="default">
        <backup-count>1</backup-count>
    </set>
    <jobtracker name="default">
        <max-thread-size>0</max-thread-size>
        <queue-size>0</queue-size>
        <retry-count>0</retry-count>
        <chunk-size>1000</chunk-size>
        <communicate-stats>true</communicate-stats>
        <topology-changed-strategy>CANCEL_RUNNING_OPERATION</topology-changed-strategy>
    </jobtracker>
    <semaphore name="default">
        <initial-permits>0</initial-permits>
        <backup-count>1</backup-count>
        <async-backup-count>0</async-backup-count>
    </semaphore>
    <serialization>
        <portable-version>0</portable-version>
    </serialization>
    <services enable-defaults="true" />
</hazelcast>
```

<br></br>