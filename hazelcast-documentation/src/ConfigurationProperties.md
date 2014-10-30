## Advanced Configuration Properties

Hazelcast has advanced configuration properties to tune some aspects of it. These can be set as property name and value pairs through declarative configuration, programmatic configuration or JVM system property.

### Declarative Configuration

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
### Programmatic Configuration

```java
Config config = new Config() ;
config.setProperty( "hazelcast.property.foo", "value" );
```

### System Property

1. Using JVM parameter: `java -Dhazelcast.property.foo=value`

2. Using System class: `System.setProperty( "hazelcast.property.foo", "value" );`

Below table lists the advanced configuration properties with their descriptions.



Property Name | Default Value | Type | Description
:--------------|:---------------|:------|:------------
`hazelcast.health.monitoring.level`|SILENT|string|Health monitoring log level. When *SILENT*, logs are printed only when values exceed some predefined threshold. When *NOISY*, logs are always printed periodically. Set *OFF* to turn off completely.  
`hazelcast.health.monitoring.delay.seconds`|30|int|Health monitoring logging interval in seconds.
`hazelcast.version.check.enabled` | true | bool  |   Enable Hazelcast new version check on startup.
`hazelcast.prefer.ipv4.stack` | true | bool  |   Prefer Ipv4 network interface when picking a local address.
`hazelcast.io.thread.count` | 3 | int | Number of input and output threads.
`hazelcast.operation.thread.count` | -1 | int | Number of partition based operation handler threads. `-1` means CPU core count x 2.
`hazelcast.operation.generic.thread.count` | -1 | int | Number of generic operation handler threads. `-1` means CPU core count x 2.
`hazelcast.event.thread.count` | 5 | int | Number of event handler threads.
`hazelcast.event.queue.capacity` | 1000000 | int | Capacity of internal event queue.
`hazelcast.event.queue.timeout.millis` | 250 | int | Timeout to enqueue events to event queue.
`hazelcast.connect.all.wait.seconds` | 120 | int | Timeout to connect all other cluster members when a member is joining to a cluster.
`hazelcast.memcache.enabled`| true | bool |   Enable [Memcache](#memcache-client) client request listener service.
`hazelcast.rest.enabled` | true | bool |   Enable [REST](#rest-client) client request listener service.
`hazelcast.map.load.chunk.size` | 1000 | int |   Chunk size for [MapLoader](#persistence) 's map initialization process (MapLoder.loadAllKeys()).
`hazelcast.merge.first.run.delay.seconds` | 300 | int |   Initial run delay of [split brain/merge process](#network-partitioning-split-brain-syndrome) in seconds.
`hazelcast.merge.next.run.delay.seconds` | 120 | int |   Run interval of [split brain/merge process](#network-partitioning-split-brain-syndrome) in seconds.
`hazelcast.operation.call.timeout.millis`| 60000 | int | Timeout to wait for a response when a remote call is sent, in milliseconds.
`hazelcast.socket.bind.any` | true | bool | Bind both server-socket and client-sockets to any local interface.
`hazelcast.socket.server.bind.any` | true | bool | Bind server-socket to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.
`hazelcast.socket.client.bind.any` | true | bool |   Bind client-sockets to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.
`hazelcast.socket.client.bind`|true|bool|Bind client socket to an interface when connecting to a remote server socket. When set to `false`, client socket is not bound to any interface.
`hazelcast.socket.receive.buffer.size` | 32 | int | Socket receive buffer (`SO_RCVBUF`) size in KB.
`hazelcast.socket.send.buffer.size` | 32 | int  | Socket send buffer (`SO_SNDBUF`) size in KB.
`hazelcast.socket.linger.seconds`|0|int|Set socket `SO_LINGER` option.
`hazelcast.socket.keep.alive` | true | bool  | Socket set keep alive (`SO_KEEPALIVE`).
`hazelcast.socket.no.delay` | true | bool  |   Socket set TCP no delay.
`hazelcast.shutdownhook.enabled` | true | bool  | Enable Hazelcast shutdownhook thread.
`hazelcast.wait.seconds.before.join` | 5 | int  | Wait time before join operation.
`hazelcast.max.join.seconds`|300|int| Join timeout, maximum time to try to join before giving.
`hazelcast.max.join.merge.target.seconds`|20|int|Split-brain merge timeout for a specific target.
`hazelcast.max.wait.seconds.before.join` | 20 | int  |   Maximum wait time before join operation.
`hazelcast.heartbeat.interval.seconds` | 1 | int  |   Heartbeat send interval in seconds.
`hazelcast.max.no.heartbeat.seconds` | 500 | int  |   Max timeout of heartbeat in seconds for a node to assume it is dead.
`hazelcast.max.no.master.confirmation.seconds` | 450 | int  |   Max timeout of master confirmation from other nodes.
`hazelcast.master.confirmation.interval.seconds` | 30 | int  |   Interval at which nodes send master confirmation.
`hazelcast.member.list.publish.interval.seconds` | 600 | int  |   Interval at which master node publishes a member list.
`hazelcast.icmp.enabled` | false | bool  |   Enable ICMP ping.
`hazelcast.icmp.timeout` | 1000 | int |   ICMP timeout in ms.
`hazelcast.icmp.ttl` | 0 | int |   ICMP TTL (maximum numbers of hops to try).
`hazelcast.initial.min.cluster.size` | 0 | int  |   Initial expected cluster size to wait before node to start completely.
`hazelcast.initial.wait.seconds` | 0 | int  |   Initial time in seconds to wait before node to start completely.
`hazelcast.map.replica.wait.seconds.for.scheduled.tasks`|10|int|Scheduler delay for map tasks those will be executed on backup members.
`hazelcast.partition.count` | 271 | int  |   Total partition count.
`hazelcast.logging.type` | jdk | enum |   Name of [logging](#logging-configuration) framework type to send logging events.
`hazelcast.jmx` | false | bool  |   Enable [JMX](#monitoring-with-jmx) agent.
`hazelcast.jmx.detailed` | false | bool  |   Enable detailed views on [JMX](#monitoring-with-jmx).
`hazelcast.mc.max.visible.instance.count` | 100 | int  |   Management Center maximum visible instance count.
`hazelcast.mc.url.change.enabled` | true | bool  |   Management Center changing server url is enabled.
`hazelcast.connection.monitor.interval` | 100 | int  |   Minimum interval to consider a connection error as critical in milliseconds.
`hazelcast.connection.monitor.max.faults` | 3 | int  |   Maximum IO error count before disconnecting from a node.
`hazelcast.partition.migration.interval` | 0 | int |   Interval to run partition migration tasks in seconds.
`hazelcast.partition.migration.timeout` | 300 | int  |   Timeout for partition migration tasks in seconds.
`hazelcast.partition.migration.zip.enabled`|true|bool|Enable compression during partition migration.
`hazelcast.partition.table.send.interval`|15|int|Interval for publishing partition table periodically to all cluster members.
`hazelcast.partition.backup.sync.interval`|30|int|Interval for syncing backup replicas.
`hazelcast.partitioning.strategy.class`|null|string|Class name implementing `com.hazelcast.core.PartitioningStrategy`, which defines key to partition mapping.
`hazelcast.migration.min.delay.on.member.removed.seconds`|5|int|Minimum delay (in seconds) between detection of a member that has left and start of the rebalancing process.
`hazelcast.graceful.shutdown.max.wait` | 600 | int  |   Maximum wait seconds during graceful shutdown.
`hazelcast.system.log.enabled` | true | bool  |   Enable system logs.
`hazelcast.elastic.memory.enabled` | false | bool  |   Enable [Hazelcast Elastic Memory](#hazelcast-elastic-memory) storage.
`hazelcast.elastic.memory.total.size` | 128 | int  |   [Hazelcast Elastic Memory](#hazelcast-elastic-memory) storage total size in MB.
`hazelcast.elastic.memory.chunk.size` | 1 | int  |   [Hazelcast Elastic Memory](#hazelcast-elastic-memory) storage chunk size in KB.
`hazelcast.elastic.memory.shared.storage` | false | bool  |   Enable [Hazelcast Elastic Memory](#hazelcast-elastic-memory) shared storage.
`hazelcast.elastic.memory.unsafe.enabled`|false|bool|Enable usage of `sun.misc.Unsafe` when allocating, reading and modifying elastic memory storage.
`hazelcast.enterprise.license.key` | null | string  |   [Hazelcast Enterprise](http://www.hazelcast.com/products.jsp) license key.
`hazelcast.client.heartbeat.timeout`|300000|string|Timeout for the heartbeat messages sent by the client to members. If there is no any message passing between client and member within the given time via this property in milliseconds the connection will be closed.
`hazelcast.client.heartbeat.interval`|10000|string|The frequency of heartbeat messages sent by the clients to members.
`hazelcast.client.max.failed.heartbeat.count`|3|string|When the count of failed heartbeats sent to members reaches this value, the cluster is deemed as dead by the client.
`hazelcast.client.request.retry.count`|20|string|The retry count of the connection requests by the client to the members.
`hazelcast.client.request.retry.wait.time`|250|string|The frequency of the connection retries.
`hazelcast.client.event.thread.count`|5|string|Thread count for handling incoming event packets.
`hazelcast.client.event.queue.capacity`|1000000|string|Default value of the capacity of executor that handles incoming event packets.

