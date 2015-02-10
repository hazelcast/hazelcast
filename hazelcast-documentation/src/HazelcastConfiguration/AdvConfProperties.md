## Advanced Configuration Properties

Hazelcast has advanced configuration properties to tune some aspects of it. These can be set as property name and value pairs through declarative configuration, programmatic configuration or JVM system property.

**Declarative Configuration**

```xml
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config
    http://www.hazelcast.com/schema/config/hazelcast-config-3.0.xsd"
    xmlns="http://www.hazelcast.com/schema/config"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

  ....
  <properties>
    <property name="hazelcast.property.foo">value</property>
    ....
  </properties>
</hazelcast>
```
**Programmatic Configuration**

```java
Config config = new Config() ;
config.setProperty( "hazelcast.property.foo", "value" );
```

**System Property**

1. Using JVM parameter: `java -Dhazelcast.property.foo=value`

2. Using System class: `System.setProperty( "hazelcast.property.foo", "value" );`

Below table lists the advanced configuration properties with their descriptions in alphabetical order.



Property Name | Default Value | Type | Description
:--------------|:---------------|:------|:------------
`hazelcast.backpressure.enabled`|false|bool|Enable back pressure.
`hazelcast.backpressure.syncwindow`|1000|string|Used when back pressure is enabled. The larger the sync window value, the less frequent a asynchronous backup is converted to a sync backup.
`hazelcast.clientengine.thread.count`||int|Maximum number of threads to process non-partition-aware client requests, like `map.size()`, query, executor tasks, etc. Default count is 20 times number of cores.
`hazelcast.client.event.queue.capacity`|1000000|string|Default value of the capacity of executor that handles incoming event packets.
`hazelcast.client.event.thread.count`|5|string|Thread count for handling incoming event packets.
`hazelcast.client.heartbeat.interval`|10000|string|The frequency of heartbeat messages sent by the clients to members.
`hazelcast.client.heartbeat.timeout`|300000|string|Timeout for the heartbeat messages sent by the client to members. If there is not any message passing between client and member within the given time via this property in milliseconds the connection will be closed.
`hazelcast.client.invocation.timeout.seconds`|120|string|Time to give up the invocation when a member in the member list is not reachable.
`hazelcast.client.shuffle.member.list`|true|string|The client shuffles the given member list to prevent all clients to connect to the same node when this property is `false`. When it is set to `true`, the client tries to connect to the nodes in the given order.
`hazelcast.connect.all.wait.seconds` | 120 | int | Timeout to connect all other cluster members when a member is joining to a cluster.
`hazelcast.connection.monitor.interval` | 100 | int  |   Minimum interval to consider a connection error as critical in milliseconds.
`hazelcast.connection.monitor.max.faults` | 3 | int  |   Maximum IO error count before disconnecting from a node.
`hazelcast.enterprise.license.key` | null | string  |   [Hazelcast Enterprise](http://www.hazelcast.com/products.jsp) license key.
`hazelcast.enterprise.wanrep.queuesize`|100000|int| Queue size for the WAN replication.
`hazelcast.event.queue.capacity` | 1000000 | int | Capacity of internal event queue.
`hazelcast.event.queue.timeout.millis` | 250 | int | Timeout to enqueue events to event queue.
`hazelcast.event.thread.count` | 5 | int | Number of event handler threads.
`hazelcast.graceful.shutdown.max.wait` | 600 | int  |   Maximum wait seconds during graceful shutdown.
`hazelcast.health.monitoring.delay.seconds`|30|int|Health monitoring logging interval in seconds.
`hazelcast.health.monitoring.level`|SILENT|string|Health monitoring log level. When *SILENT*, logs are printed only when values exceed some predefined threshold. When *NOISY*, logs are always printed periodically. Set *OFF* to turn off completely.
`hazelcast.heartbeat.interval.seconds` | 1 | int  |   Heartbeat send interval in seconds.
`hazelcast.icmp.enabled` | false | bool  |   Enable ICMP ping.
`hazelcast.icmp.timeout` | 1000 | int |   ICMP timeout in ms.
`hazelcast.icmp.ttl` | 0 | int |   ICMP TTL (maximum numbers of hops to try).
`hazelcast.initial.min.cluster.size` | 0 | int  |   Initial expected cluster size to wait before node to start completely.
`hazelcast.initial.wait.seconds` | 0 | int  |   Initial time in seconds to wait before node to start completely.
`hazelcast.io.thread.count` | 3 | int | Number of input and output threads.
`hazelcast.jcache.provider.type`||string|Type of the JCache provider. Values can be `client` or `server`.
`hazelcast.jmx` | false | bool  |   Enable [JMX](#monitoring-with-jmx) agent.
`hazelcast.jmx.detailed` | false | bool  |   Enable detailed views on [JMX](#monitoring-with-jmx).
`hazelcast.logging.type` | jdk | enum |   Name of [logging](#logging-configuration) framework type to send logging events.
`hazelcast.map.load.chunk.size` | 1000 | int |   Chunk size for [MapLoader](#persistence) 's map initialization process (MapLoder.loadAllKeys()).
`hazelcast.map.replica.wait.seconds.for.scheduled.tasks`|10|int|Scheduler delay for map tasks those will be executed on backup members.
`hazelcast.map.write.behind.queue.capacity`|50000|string|Maximum write-behind queue capacity per node. It is the total of all write-behind queue sizes in a node including backups. Its maximum value is `Integer.MAX_VALUE`. The value of this property is taken into account only if the `write-coalescing` element of the Map Store configuration is `false`. Please refer to the [Map Store section](#map-store) for the description of the `write-coalescing` element.
`hazelcast.master.confirmation.interval.seconds` | 30 | int  |   Interval at which nodes send master confirmation.
`hazelcast.max.join.merge.target.seconds`|20|int|Split-brain merge timeout for a specific target.
`hazelcast.max.join.seconds`|300|int| Join timeout, maximum time to try to join before giving.
`hazelcast.max.no.heartbeat.seconds` | 500 | int  |   Max timeout of heartbeat in seconds for a node to assume it is dead.
`hazelcast.max.no.master.confirmation.seconds` | 450 | int  |   Max timeout of master confirmation from other nodes.
`hazelcast.max.wait.seconds.before.join` | 20 | int  |   Maximum wait time before join operation.
`hazelcast.mc.max.visible.instance.count` | 100 | int  |   Management Center maximum visible instance count.
`hazelcast.mc.url.change.enabled` | true | bool  |   Management Center changing server url is enabled.
`hazelcast.member.list.publish.interval.seconds` | 600 | int  |   Interval at which master node publishes a member list.
`hazelcast.memcache.enabled`| true | bool |   Enable [Memcache](#memcache-client) client request listener service.
`hazelcast.merge.first.run.delay.seconds` | 300 | int |   Initial run delay of [split brain/merge process](#network-partitioning-split-brain-syndrome) in seconds.
`hazelcast.merge.next.run.delay.seconds` | 120 | int |   Run interval of [split brain/merge process](#network-partitioning-split-brain-syndrome) in seconds.
`hazelcast.migration.min.delay.on.member.removed.seconds`|5|int|Minimum delay (in seconds) between detection of a member that has left and start of the rebalancing process.
`hazelcast.operation.backup.timeout.millis`|5|int|Maximum time a caller to wait for backup responses of an operation. After this timeout, operation response will be returned to the caller even no backup response is received.
`hazelcast.operation.call.timeout.millis`| 60000 | int | Timeout to wait for a response when a remote call is sent, in milliseconds.
`hazelcast.operation.generic.thread.count` | -1 | int | Number of generic operation handler threads. `-1` means CPU core count x 2.
`hazelcast.operation.thread.count` | -1 | int | Number of partition based operation handler threads. `-1` means CPU core count x 2.
`hazelcast.partition.backup.sync.interval`|30|int|Interval for syncing backup replicas.
`hazelcast.partition.count` | 271 | int  |   Total partition count.
`hazelcast.partition.max.parallel.replications`|5|int|Maximum number of parallel partition backup replication operations per node. When a partition backup ownership changes or a backup inconsistency is detected, the nodes start to sync their backup partitions. This parameter limits the maximum running replication operations in parallel.
`hazelcast.partition.migration.interval` | 0 | int |   Interval to run partition migration tasks in seconds.
`hazelcast.partition.migration.timeout` | 300 | int  |   Timeout for partition migration tasks in seconds.
`hazelcast.partition.migration.zip.enabled`|true|bool|Enable compression during partition migration.
`hazelcast.partition.table.send.interval`|15|int|Interval for publishing partition table periodically to all cluster members.
`hazelcast.partitioning.strategy.class`|null|string|Class name implementing `com.hazelcast.core.PartitioningStrategy`, which defines key to partition mapping.
`hazelcast.performance.monitoring.enabled`||bool|Enable the performance monitor which is a tool enabling you to see internal performance metrics. These metrics are written to the log file.
`hazelcast.performance.monitoring.delay.seconds`||int| The delay in seconds between monitoring of the performance.
`hazelcast.prefer.ipv4.stack` | true | bool  |   Prefer Ipv4 network interface when picking a local address.
`hazelcast.rest.enabled` | true | bool |   Enable [REST](#rest-client) client request listener service.
`hazelcast.shutdownhook.enabled` | true | bool  | Enable Hazelcast shutdownhook thread.
`hazelcast.socket.bind.any` | true | bool | Bind both server-socket and client-sockets to any local interface.
`hazelcast.socket.client.bind`|true|bool|Bind client socket to an interface when connecting to a remote server socket. When set to `false`, client socket is not bound to any interface.
`hazelcast.socket.client.bind.any` | true | bool |   Bind client-sockets to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.
`hazelcast.socket.connect.timeout.seconds`|0|int|Socket connection timeout in seconds. `Socket.connect()` will be blocked until either connection is established or connection is refused or this timeout passes. Default is 0, means infinite. 
`hazelcast.socket.keep.alive` | true | bool  | Socket set keep alive (`SO_KEEPALIVE`).
`hazelcast.socket.linger.seconds`|0|int|Set socket `SO_LINGER` option.
`hazelcast.socket.no.delay` | true | bool  |   Socket set TCP no delay.
`hazelcast.socket.receive.buffer.size` | 32 | int | Socket receive buffer (`SO_RCVBUF`) size in KB.
`hazelcast.socket.send.buffer.size` | 32 | int  | Socket send buffer (`SO_SNDBUF`) size in KB.
`hazelcast.socket.server.bind.any` | true | bool | Bind server-socket to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.
`hazelcast.system.log.enabled` | true | bool  |   Enable system logs.
`hazelcast.version.check.enabled` | true | bool  |   Enable Hazelcast new version check on startup.
`hazelcast.wait.seconds.before.join` | 5 | int  | Wait time before join operation.

