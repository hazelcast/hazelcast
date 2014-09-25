

## Configuration

Hazelcast can be configured declaratively (XML) or programmatically (API) or even by the mix of both.

**1- Declarative Configuration**

If you are creating new Hazelcast instance with passing `null` parameter to `Hazelcast.newHazelcastInstance(null)` or just using empty factory method (`Hazelcast.newHazelcastInstance()`), Hazelcast will look into two places for the configuration file:

-   **System property:** Hazelcast will first check if "`hazelcast.config`" system property is set to a file path. Example: `-Dhazelcast.config=C:/myhazelcast.xml`.

-   **Classpath:** If config file is not set as a system property, Hazelcast will check classpath for `hazelcast.xml` file.

If Hazelcast does not find any configuration file, it will happily start with default configuration (`hazelcast-default.xml`) located in `hazelcast.jar`. (Before configuring Hazelcast, please try to work with default configuration to see if it works for you. Default should be just fine for most of the users. If not, then consider custom configuration for your environment.)

If you want to specify your own configuration file to create `Config`, Hazelcast supports several ways including filesystem, classpath, InputStream, URL, etc.:

-   `Config cfg = new XmlConfigBuilder(xmlFileName).build();`

-   `Config cfg = new XmlConfigBuilder(inputStream).build();`

-   `Config cfg = new ClasspathXmlConfig(xmlFileName);`

-   `Config cfg = new FileSystemXmlConfig(configFilename);`

-   `Config cfg = new UrlXmlConfig(url);`

-   `Config cfg = new InMemoryXmlConfig(xml);`



**2- Programmatic Configuration**

To configure Hazelcast programmatically, just instantiate a `Config` object and set/change its properties/attributes due to your needs. Just to give an idea, below is a sample code in which some network, map, map store and near cache attributes are configured for a Hazelcast instance.

```java
Config config = new Config();
config.getNetworkConfig().setPort( 5900 );
config.getNetworkConfig().setPortAutoIncrement( false );
        
NetworkConfig network = config.getNetworkConfig();
JoinConfig join = network.getJoin();
join.getMulticastConfig().setEnabled( false );
join.getTcpIpConfig().addMember( "10.45.67.32" ).addMember( "10.45.67.100" )
            .setRequiredMember( "192.168.10.100" ).setEnabled( true );
network.getInterfaces().setEnabled( true ).addInterface( "10.45.67.*" );
        
MapConfig mapConfig = new MapConfig();
mapConfig.setName( "testMap" );
mapConfig.setBackupCount( 2 );
mapConfig.getMaxSizeConfig().setSize( 10000 );
mapConfig.setTimeToLiveSeconds( 300 );
        
MapStoreConfig mapStoreConfig = new MapStoreConfig();
mapStoreConfig.setClassName( "com.hazelcast.examples.DummyStore" )
    .setEnabled( true );
mapConfig.setMapStoreConfig( mapStoreConfig );

NearCacheConfig nearCacheConfig = new NearCacheConfig();
nearCacheConfig.setMaxSize( 1000 ).setMaxIdleSeconds( 120 )
    .setTimeToLiveSeconds( 300 );
mapConfig.setNearCacheConfig( nearCacheConfig );

config.addMapConfig( mapConfig );
```

After creating `Config` object, you can use it to create a new Hazelcast instance.

-   `HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance( config );`

-   To create a named `HazelcastInstance` you should set `instanceName` of `Config` object. 

```java
    Config config = new Config();
    config.setInstanceName( "my-instance" );
    Hazelcast.newHazelcastInstance( config );
    ```
-   To retrieve an existing `HazelcastInstance` using its name, use;

    `Hazelcast.getHazelcastInstanceByName( "my-instance" );`

-   To retrieve all existing `HazelcastInstance`s, use;

    `Hazelcast.getAllHazelcastInstances();`




Rest of the chapter will first explain the configuration items listed below.

- Network 
- Group
- Map
- MultiMap
- Queue
- Topic
- List
- Set
- Semaphore
- Executor Service
- Serialization
- Partition Group
- Jobtracker
- Services
- Management Center


Then, it will talk about Listener and Logging configurations. And finally, the chapter will end with the advanced system property definitions.

***ATTENTION:*** *Most of the sections below use the tags used in declarative configuration when explaining configuration items. We are assuming that the reader is familiar with their programmatic equivalents, since both approaches have the similar tag/method names (e.g. `port-count` tag in declarative configuration is equivalent to `setPortCount` in programmatic configuration).*

### Network Configuration

All network related configuration is performed via `network` tag in the XML file or the class `NetworkConfig` when using programmatic configuration. Let's first give the samples for these two approaches. Then we will look at its parameters, which are a lot.

**Declarative:**


```xml
   <network>
        <public-address> ??? </public-address>
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

- public-address
- port
- outbound-ports
- join
- interfaces
- ssl
- socket-interceptor
- symmetric-encryption

##### Public Address

It is used to override public address of a node. By default, a node selects its socket address as its public address. But behind a network address translation (NAT), two endpoints (nodes) may not be able to see/access each other. If both nodes set their public addresses to their defined addresses on NAT, then that way they can communicate with each other. In this case, their public addresses are not an address of a local network interface but a virtual address defined by NAT. It is optional to set and useful when you have a private cloud.

##### Port

You can specify the ports which Hazelcast will use to communicate between cluster members. Its default value is `5701`. Sample configurations are shown below.

**Declarative:**

```xml
<network>
  <port port-count="20" auto-increment="false">5701</port>
</network>
```

**Programmatic:**

```java
Config config = new Config();
config.getNetworkConfig().setPort( "5900" ); 
             .setPortCount( "20" ).setPortAutoIncrement( "false" );
```

It has below attributes.

- `port-count`: By default, Hazelcast will try 100 ports to bind. Meaning that, if you set the value of port as 5701, as members are joining to the cluster, Hazelcast tries to find ports between 5701 and 5801. You can choose to change the port count in the cases like having large instances on a single machine or willing to have only a few ports to be assigned. The parameter `port-count` is used for this purpose, whose default value is 100.



- `auto-increment`: According to the above example, Hazelcast will try to find free ports between 5781 and 5801. Normally, you will not need to change this value, but it will come very handy when needed. You may also want to choose to use only one port. In that case, you can disable the auto-increment feature of `port` by setting its value as `false`.


Naturally, the parameter `port-count` is ignored when the above configuration is made.

##### Outbound Ports


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


##### Join

This configuration parameter is used to enable the Hazelcast instances to form a cluster, i.e. to join the members. Three ways can be used to join the members: TCP/IP, multicast and AWS (EC2). Below are sample configurations.

**Declarative:**

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
                <required-member>192.168.1.104</required-member>
                <member>192.168.1.104</member>
                <members>192.168.1.105,192.168.1.106</members>
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
join.getMulticastConfig().setEnabled( "false" )
            .addTrustedInterface( "192.168.1.102" );
join.getTcpIpConfig().addMember( "10.45.67.32" ).addMember( "10.45.67.100" )
            .setRequiredMember( "192.168.10.100" ).setEnabled( true );
```

It has below elements and attributes.

- `multicast`: It includes parameters to fine tune the multicast join mechanism.
	- `enabled`: Specifies whether the multicast discovery is enabled or not. Values can be `true` or `false`.
	- `multicast-group`: The multicast group IP address. Specify it when you want to create clusters within the same network. Values can be between 224.0.0.0 and 239.255.255.255. Default value is 224.2.2.3
	- `multicast-port`: The multicast socket port which Hazelcast member listens to and sends discovery messages through it. Default value is 54327.
	- `multicast-time-to-live`: Time-to-live value for multicast packets sent out to control the scope of multicasts. You can have more information [here](http://www.tldp.org/HOWTO/Multicast-HOWTO-2.html).
	- `multicast-timeout-seconds`: Only when the nodes are starting up, this timeout (in seconds) specifies the period during which a node waits for a multicast response from another node. For example, if you set it as 60 seconds, each node will wait for 60 seconds until a leader node is selected. Its default value is 2 seconds. 
	- `trusted-interfaces`: Includes IP addresses of trusted members. When a node wants to join to the cluster, its join request will be rejected if it is not a trusted member. You can give an IP addresses range using the wildcard (\*) on the last digit of IP address (e.g. 192.168.1.\* or 192.168.1.100-110).
	
- `tcp-ip`: It includes parameters to fine tune the TCP/IP join mechanism.
	- `enabled`: Specifies whether the TCP/IP discovery is enabled or not. Values can be `true` or `false`.
	- `required-member`: IP address of the required member. Cluster will only formed if the member with this IP address is found.
	- `member`: IP address(es) of one or more well known members. Once members are connected to these well known ones, all member addresses will be communicated with each other. You can also give comma separated IP addresses using the `members` tag.

- `aws`: It includes parameters to allow the nodes form a cluster on Amazon EC2 environment.
	- `enabled`: Specifies whether the EC2 discovery is enabled or not. Values can be `true` or `false`.
	- `access-key`, `secret-key`: Access and secret keys of your account on EC2.
	- `region`: The region where your nodes are running. Default value is `us-east-1`. Needs to be specified if the region is other than the default one.
	- `host-header`: ???. It is optional.
	- `security-group-name`:Name of the security group you specified at the EC2 management console. It is used to narrow the Hazelcast nodes to be within this group. It is optional.
	- `tag-key`, `tag-value`: To narrow the members in the cloud down to only Hazelcast nodes, you can set these parameters as the ones you specified in the EC2 console. They are optional.

##### Interfaces

You can specify which network interfaces that Hazelcast should use. Servers mostly have more than one network interface so you may want to list the valid IPs. Range characters ('\*' and '-') can be used for simplicity. So 10.3.10.\*, for instance, refers to IPs between 10.3.10.0 and 10.3.10.255. Interface 10.3.10.4-18 refers to IPs between 10.3.10.4 and 10.3.10.18 (4 and 18 included). If network interface configuration is enabled (disabled by default) and if Hazelcast cannot find an matching interface, then it will print a message on console and will not start on that node.

**Declarative:**

```xml
<hazelcast>
  ...
  <network>
    ...
    <interfaces enabled="true">
      <interface>10.3.16.*</interface> 
      <interface>10.3.10.4-18</interface> 
      <interface>192.168.1.3</interface>         
    </interfaces>    
  </network>
  ...
</hazelcast> 
```

**Programmatic:**

```java
Config config = new Config();
NetworkConfig network = config.getNetworkConfig();
InterfacesConfig interface = network.getInterfaces();
interface.setEnabled( "true" )
            .addInterface( "192.168.1.3" );
```




##### SSL

This is a Hazelcast Enterprise feature, please see [Security](#security) chapter.

##### Socket Interceptor

This is a Hazelcast Enterprise feature, please see [Security](#security) chapter.

##### Symmetric Encryption

This is a Hazelcast Enterprise feature, please see [Security](#security) chapter.



### Group Configuration

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


### Map Configuration

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

### Multimap Configuration

This configuration is for ???. It has below attributes.

- backup-count:
- async-backup-count:
- statistics-enabled:
- value-collection-type:
- entry-listeners:
- partition-strategy:


### Queue Configuration

**Declarative:**

```xml
<queue name="default">
    <max-size>0</max-size>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <empty-queue-ttl>-1</empty-queue-ttl>
    <item-listeners>
       <item-listener>???</item-listener>
    <item-listeners>
</queue>
<queue-store>
    <class-name>com.hazelcast.QueueStoreImpl</class-name>
    <properties>
       <property name="binary">false</property>
       <property name="memory-limit">10000</property>
       <property name="bulk-load">500</property>
    </properties>
</queue-store>   
```

**Programmatic:**

```java
Config config = new Config();
QueueConfig queueConfig = config.getQueueConfig();
queueConfig.setName( "MyQueue" ).setBackupCount( "1" )
        .setMaxSize( "0" ).setStatisticsEnabled( "true" );
queueConfig.getQueueStoreConfig()
        .setEnabled ( "true" )
        .setClassName( "com.hazelcast.QueueStoreImpl" )
        .setProperty( "binary", "false" );
```

It has below attributes and parameters.

- `max-size`: Value of maximum size of items in the Queue.
- `backup-count`: Count of synchronous backups. Remember that, Queue is a non-partitioned data structure, i.e. all entries of a Set resides in one partition. When this parameter is '1', it means there will be a backup of that Set in another node in the cluster. When it is '2', 2 nodes will have the backup.
- `async-backup-count`: Count of asynchronous backups.
- `empty-queue-ttl`: Value of time to live to empty the Queue.
- `item-listeners`: ???
- `queue-store`: Includes the queue store factory class name and the properties  *binary*, *memory limit* and *bulk load*. Please refer to [Queue Persistence](#queue-persistence).
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this Queue using the method `getLocalQueueStats()`.

### Topic Configuration

**Declarative Configuration:**

```xml
<hazelcast>
  ...
  <topic name="yourTopicName">
    <global-ordering-enabled>true</global-ordering-enabled>
    <statistics-enabled>true</statistics-enabled>
    <message-listeners>
      <message-listener>MessageListenerImpl</message-listener>
    </message-listeners>
  </topic>
  ...
</hazelcast>
```

**Programmatic Configuration:**

```java
TopicConfig topicConfig = new TopicConfig();
topicConfig.setGlobalOrderingEnabled( true );
topicConfig.setStatisticsEnabled( true );
topicConfig.setName( "yourTopicName" );
MessageListener<String> implementation = new MessageListener<String>() {
  @Override
  public void onMessage( Message<String> message ) {
    // process the message
  }
};
topicConfig.addMessageListenerConfig( new ListenerConfig( implementation ) );
HazelcastInstance instance = Hazelcast.newHazelcastInstance()
```


It has below attributes.

- statistics-enabled: By default, it is **true**, meaning statistics are calculated.
- global-ordering-enabled: By default, it is **false**, meaning there is no global order guarantee by default.???
- message-listeners: ???



Topic related but not topic specific configuration parameters

   - `hazelcast.event.queue.capacity`: default value is 1,000,000.
   - `hazelcast.event.queue.timeout.millis`: default value is 250.
   - `hazelcast.event.thread.count`: default value is 5.
   
<br></br>
***RELATED INFORMATION*** 

*For description of these parameters, please see [Global Event Configuration](#global-event-configuration)*





### Set Configuration

**Declarative:**

```xml
<set name="default">
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <max-size>10</max-size>
   <statistics-enabled>true</statistics-enabled>
   <item-listeners>
      <item-listener>???<item-listener>
   </item-listeners>
</set>
```

**Programmatic:**

```java
Config config = new Config();
CollectionConfig collectionSet = config.getCollectionConfig();
collectionSet.setName( "MySet" ).setBackupCount( "1" )
        .setMaxSize( "10" ).setStatisticsEnabled( "true" );
```
   

It has below parameters.


- `backup-count`: Count of synchronous backups. Remember that, Set is a non-partitioned data structure, i.e. all entries of a Set resides in one partition. When this parameter is '1', it means there will be a backup of that Set in another node in the cluster. When it is '2', 2 nodes will have the backup.
- `async-backup-count`: Count of asynchronous backups.
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this Set.
- `max-size`: It is the maximum entry size for this Set.
- `item-listeners`: ???



### List Configuration

**Declarative:**

```xml
<list name="default">
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <max-size>10</max-size>
   <statistics-enabled>true</statistics-enabled>
   <item-listeners>
      <item-listener>???<item-listener>
   </item-listeners>
</list>
```

**Programmatic:**

```java
Config config = new Config();
CollectionConfig collectionList = config.getCollectionConfig();
collectionList.setName( "MyList" ).setBackupCount( "1" )
        .setMaxSize( "10" ).setStatisticsEnabled( "true" );
```
   

It has below parameters.


- `backup-count`: Count of synchronous backups. Remember that, List is a non-partitioned data structure, i.e. all entries of a List resides in one partition. When this parameter is '1', it means there will be a backup of that List in another node in the cluster. When it is '2', 2 nodes will have the backup.
- `async-backup-count`: Count of asynchronous backups.
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this List.
- `max-size`: It is the maximum entry size for this List.
- `item-listeners`: ???




### Semaphore Configuration

**Declarative:**

```xml
<semaphore name="semaphore">
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <initial-permits>3</initial-permits>
</semaphore>
```

**Programmatic:**

```java
Config config = new Config();
SemaphoreConfig semaphoreConfig = config.getSemaphoreConfig();
semaphoreConfig.setName( "semaphore" ).setBackupCount( "1" )
        .setInitialPermits( "3" );
```

It has below attributes.

- initial-permits: It is the thread count which the concurrent access is limited to. For example, if it is set as "3", concurrent access to the object is limited to 3 thread.
- backup-count: Count of synchronous backups. ???
- async-backup-count: Count of asynchronous backups. ???

### Executor Service Configuration

This configuration is for ???. It has below attributes.

- pool-size: The number of executor threads per Member for the Executor.
- queue-capacity: Capacity of the queue.
- statistics-enabled:


### Serialization Configuration

This configuration is for ???. It has below attributes.

- portable-version:




### Partition Group Configuration

When you enable partition grouping, Hazelcast presents three choices to configure partition groups at the moment.

-   First one is to group nodes automatically using IP addresses of nodes, so nodes sharing same network interface will be grouped together. All members on the same host (IP address or domain name) will be a single partition group. This helps to avoid data loss when a physical server crashes by not storing multiple replicas of the same partition on the same host. But if there are multiple network interfaces or domain names per physical machine, that will make this assumption invalid.

```xml
<partition-group enabled="true" group-type="HOST_AWARE" />
```

```java
Config config = ...;
PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
partitionGroupConfig.setEnabled( true )
    .setGroupType( MemberGroupType.HOST_AWARE );
```

-   Second one is custom grouping using Hazelcast's interface matching configuration. This way, you can add different and multiple interfaces to a group. You can also use wildcards in interface addresses. For example, the users can create rack aware or data warehouse partition groups using custom partition grouping.

```xml
<partition-group enabled="true" group-type="CUSTOM">
<member-group>
  <interface>10.10.0.*</interface>
  <interface>10.10.3.*</interface>
  <interface>10.10.5.*</interface>
</member-group>
<member-group>
  <interface>10.10.10.10-100</interface>
  <interface>10.10.1.*</interface>
  <interface>10.10.2.*</interface>
</member-group>
</partition-group>
```

```java
Config config = ...;
PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
partitionGroupConfig.setEnabled( true )
    .setGroupType( MemberGroupType.CUSTOM );

MemberGroupConfig memberGroupConfig = new MemberGroupConfig();
memberGroupConfig.addInterface( "10.10.0.*" )
.addInterface( "10.10.3.*" ).addInterface("10.10.5.*" );

MemberGroupConfig memberGroupConfig2 = new MemberGroupConfig();
memberGroupConfig2.addInterface( "10.10.10.10-100" )
.addInterface( "10.10.1.*").addInterface( "10.10.2.*" );

partitionGroupConfig.addMemberGroupConfig( memberGroupConfig );
partitionGroupConfig.addMemberGroupConfig( memberGroupConfig2 );
```

-   Third one is to give every member their own group. Meaning that, each member is a group of its own and primary and backup partitions are distributed randomly (not on the same physical member). This gives the least amount of protection and is the default configuration for a Hazelcast cluster.

```xml
<partition-group enabled="true" group-type="PER_MEMBER" />
```

```java
Config config = ...;
PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
partitionGroupConfig.setEnabled( true )
    .setGroupType( MemberGroupType.PER_MEMBER );
```








### Jobtracker Configuration



**Declarative:**

```xml
<job-tracker name="default">
   <max-thread-size>0</max-thread-size>
   <queue-size>0</queue-size>
   <retry-count>0</retry-count>
   <chunk-size>1000</chunk-size>
   <communicate-stats>true</communicate-stats>
   <topology-changed-strategy>CANCEL_RUNNING_OPERATION</topology-changed-strategy>
</job-tracker>
```

**Programmatic:**

```java
Config config = new Config();
JobTrackerConfig JTcfg = config.getJobTrackerConfig()
JTcfg.setName( "default" ).setQueueSize( "0" )
         .setChunkSize( "1000" );
```
   

It has below parameters.


- `max-thread-size`: Configures the maximum thread pool size of the JobTracker.
- `queue-size`: Defines the maximum number of tasks that are able to wait to be processed. A value of 0 means unbounded queue. Very low numbers can prevent successful execution since job might not be correctly scheduled or intermediate chunks are lost.
- `retry-count`: Currently not used but reserved for later use where the framework will automatically try to restart / retry operations from an available save point.
- `chunk-size`: Defines the number of emitted values before a chunk is sent to the reducers. If your emitted values are big or you want to better balance your work, you might want to change this to a lower or higher value. A value of 0 means immediate transmission but remember that low values mean higher traffic costs. A very high value might cause an OutOfMemoryError to occur if emitted values not fit into heap memory before
being sent to reducers. To prevent this, you might want to use a combiner to pre-reduce values on mapping nodes.
- `communicate-stats`: Defines if statistics (for example about processed entries) are transmitted to the job emitter. This might be used to show any kind of progress to a user inside of an UI system but produces additional traffic. If not needed, you might want to deactivate this.
- `topology-changed-strategy`: Defines how the MapReduce framework will react on topology changes while executing a job. Currently, only CANCEL_RUNNING_OPERATION is fully supported which throws an exception to the job emitter (will throw a `com.hazelcast.mapreduce.TopologyChangedException`). DISCARD_AND_RESTART ???




### Services Configuration

This configuration is used for SPI. 


**Declarative:**

```xml
<services>
   <service enabled="true">
      <name>MyService</name>
      <class-name>MyServiceClass</class-name>
      <properties>
         <custom-property-1 enabled="true">100</custom-property-1>
         <custom-property-2>true</custom-property-2>
      </properties>
   </service>
</services>
```

**Programmatic:**

```java
Config config = new Config();
JobTrackerConfig JTcfg = config.getJobTrackerConfig()
   JTcfg.setName( "default" ).setQueueSize( "0" )
         .setChunkSize( "1000" )
```
   

It has below parameters.



### Management Center Configuration

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