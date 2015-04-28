## System Properties

Hazelcast has system properties to tune some aspects of Hazelcast. You can set them as property name and value pairs through declarative configuration, programmatic configuration or JVM system property.

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

The table below lists the system properties with their descriptions in alphabetical order.



Property Name | Default Value | Type | Description
:--------------|:---------------|:------|:------------
`hazelcast.application.validation.token`||string|This property can be used to verify that Hazelcast nodes only join when their application level configuration is the same. Imagine that you have multiple machines and you want to make sure that each machine that is going to join the cluster has exactly the same application level settings (e.g. settings that are not part of the Hazelcast configuration, but maybe some filepath). To prevent these machines with potential different application level configuration to form a cluster, this property can be set. <br>You could use actual values, e.g. string paths, but you can also use e.g. an md5 hash. We'll give the give the guarantee that only nodes are going to form a cluster where the token is an exact match. If this token is different, the member can't be started and therefor you will get the guarantee that all members in the cluster, will have exactly the same application validation token. This validation-token will be checked before member join the cluster.
`hazelcast.backpressure.backoff.timeout.millis`|60000|int|Control the maximum timeout in milliseconds to wait for an invocation space to be available. If an invocation cannot be made since there are too many pending invocations, then an exponential back-off is done to give the system time to deal with the backlog of invocations. This property controls how long an invocation is allowed to wait before getting a HazelcastOverloadException. The value needs to be equal to or larger than 0.
`hazelcast.backpressure.enabled`|false|bool|Enable back pressure.
`hazelcast.backpressure.max.concurrent.invocations.per.partition`|100|int|The maximum number of concurrent invocations per partition. To prevent the system overloading, Hazelcast can apply a constraint on the number of concurrent invocations. If the maximum number of concurrent invocations has exceeded and a new invocation comes in, then an exponential back-off is applied till eventually a timeout happens or there is room for the invocation. By default it is configured as 100, so with 271 partitions that would give (271+1)*100=27200 concurrent invocations from a single member. The +1 is for generic operations. No promise is made of the invocations are tracked per partition, or if there is a general pool of invocations.
`hazelcast.backpressure.syncwindow`|1000|string|Used when back pressure is enabled. The larger the sync window value, the less frequent a asynchronous backup is converted to a sync backup.
`hazelcast.clientengine.thread.count`||int|Maximum number of threads to process non-partition-aware client requests, like `map.size()`, query, executor tasks, etc. Default count is 20 times number of cores.
`hazelcast.client.event.queue.capacity`|1000000|string|Default value of the capacity of executor that handles incoming event packets.
`hazelcast.client.event.thread.count`|5|string|Thread count for handling incoming event packets.
`hazelcast.client.heartbeat.interval`|10000|string|The frequency of heartbeat messages sent by the clients to members.
`hazelcast.client.heartbeat.timeout`|300000|string|Timeout for the heartbeat messages sent by the client to members. If no messages pass between client and member within the given time via this property in milliseconds, the connection will be closed.
`hazelcast.client.invocation.timeout.seconds`|120|string|Time to give up the invocation when a member in the member list is not reachable.
`hazelcast.client.max.no.heartbeat.seconds`|int|300|???
`hazelcast.client.shuffle.member.list`|true|string|The client shuffles the given member list to prevent all clients to connect to the same node when this property is `false`. When it is set to `true`, the client tries to connect to the nodes in the given order.
`hazelcast.connect.all.wait.seconds` | 120 | int | Timeout to connect all other cluster members when a member is joining to a cluster.
`hazelcast.connection.monitor.interval` | 100 | int  |   Minimum interval in milliseconds to consider a connection error as critical.
`hazelcast.connection.monitor.max.faults` | 3 | int  |   Maximum IO error count before disconnecting from a node.
`hazelcast.enterprise.license.key` | null | string  |   [Hazelcast Enterprise](http://www.hazelcast.com/products.jsp) license key.
`hazelcast.enterprise.wanrep.batch.size`|50|int|Defines maximum number of WAN replication events to be drained and sent to the target cluster in a batch. Batches are sent in sequence to preserve the order of events. Only one batch of events is sent to a target WAN member at a time. After batch is sent, acknowledge is waited from the target cluster. If no-ack is received, same set of events is sent again to the target cluster until the ack is received. Until this process is complete, WAN replication events are stored in the WAN replication event queue.  This queue's size is limited by the `hazelcast.enterprise.wanrep.queuecapacity` system property and if queued event count exceeds the queue capacity, no back-pressure is applied and older events in the queue will start dropping. This system property is only valid for Hazelcast Enterprise.
`hazelcast.enterprise.wanrep.batchfrequency.seconds`|5|int|Defines batch sending frequency in seconds. When event size does not reach to the value specified by the `hazelcast.enterprise.wanrep.batch.size` system property in the given time period (this time period is defined by the `hazelcast.enterprise.wanrep.batchfrequency.seconds`) those events are gathered into a batch and sent to target. This system property is only valid for Hazelcast Enterprise.
`hazelcast.enterprise.wanrep.optimeout.millis`|-1|int|Defines timeout duration (in milliseconds) for a WAN replication event before retry. If confirmation is not received in the period of this timeout duration, event is resent to target cluster. This system property is only valid for Hazelcast Enterprise.
`hazelcast.enterprise.wanrep.queuecapacity`|100000|int| Queue size for the WAN replication. Replication Events are dropped when this queue capacity is reached. Having too big queue capacity may lead to OOME problems. This system property is only valid for Hazelcast Enterprise.
`hazelcast.event.queue.capacity` | 1000000 | int | Capacity of internal event queue.
`hazelcast.event.queue.timeout.millis` | 250 | int | Timeout to enqueue events to event queue.
`hazelcast.event.thread.count` | 5 | int | Number of event handler threads.
`hazelcast.graceful.shutdown.max.wait` | 600 | int  |   Maximum wait in seconds during graceful shutdown.
`hazelcast.health.monitoring.delay.seconds`|30|int|Health monitoring logging interval in seconds.
`hazelcast.health.monitoring.level`|SILENT|string|Health monitoring log level. When *SILENT*, logs are printed only when values exceed some predefined threshold. When *NOISY*, logs are always printed periodically. Set *OFF* to turn off completely.
`hazelcast.heartbeat.interval.seconds` | 1 | int  |   Heartbeat send interval in seconds.
`hazelcast.icmp.enabled` | false | bool  |   Enable ICMP ping.
`hazelcast.icmp.timeout` | 1000 | int |   ICMP timeout in milliseconds.
`hazelcast.icmp.ttl` | 0 | int |   ICMP TTL (maximum numbers of hops to try).
`hazelcast.initial.min.cluster.size` | 0 | int  |   Initial expected cluster size to wait before node to start completely.
`hazelcast.initial.wait.seconds` | 0 | int  |   Initial time in seconds to wait before node to start completely.
`hazelcast.io.balancer.interval.seconds`|20|int|Interval in seconds between IOBalancer executions. The shorter intervals will catch I/O Imbalance faster, but they will cause higher overhead. Please see the documentation comments of the IOBalancer class under the package com.hazelcast.nio.tcp.handlermigration. Default value is 20 seconds. A negative value disables the balancer.
`hazelcast.io.thread.count` | 3 | int | Number of input and output threads.
`hazelcast.jcache.provider.type`||string|Type of the JCache provider. Values can be `client` or `server`.
`hazelcast.jmx` | false | bool  |   Enable [JMX](#monitoring-with-jmx) agent.
`hazelcast.logging.type` | jdk | enum |   Name of [logging](#logging-configuration) framework type to send logging events.
`hazelcast.map.expiry.delay.seconds`|10|int|Useful to deal with some possible edge cases. For example, when using EntryProcessor, without this delay, you may see an EntryProcessor running on owner partition found a key but EntryBackupProcessor did not find it on backup. As a result of this, when backup promotes to owner, you will end up an unprocessed key.
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
`hazelcast.mc.max.visible.slow.operations.count`|10|int|Management Center maximum visible slow operations count.
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
`hazelcast.partition.table.send.interval`|15|int|Interval for publishing partition table periodically to all cluster members.
`hazelcast.partitioning.strategy.class`|null|string|Class name implementing `com.hazelcast.core.PartitioningStrategy`, which defines key to partition mapping.
`hazelcast.performance.monitoring.enabled`||bool|Enable the performance monitor which is a tool enabling you to see internal performance metrics. These metrics are written to the log file.
`hazelcast.performance.monitoring.delay.seconds`||int| The delay in seconds between monitoring of the performance.
`hazelcast.prefer.ipv4.stack` | true | bool  |   Prefer Ipv4 network interface when picking a local address.
`hazelcast.query.max.local.partition.limit.for.precheck`|3|int|Maximum value of local partitions to trigger local pre-check for TruePredicate query operations on maps. To limit the result size of a query, a local pre-check on the requesting node can be done, before the query is sent to the cluster. Since this may increase the latency, the pre-check is limited to a maximum number of local partitions. By increasing this parameter you can prevent the execution of the query on the cluster by increasing the latency due to the prolonged local pre-check. The pre-check can be disabled by setting a value of -1.
`hazelcast.query.result.size.limit`|-1|int|Result size limit for query operations on maps. This value defines the maximum number of returned elements for a single query result. If a query exceeds this number of elements, a QueryResultSizeExceededException will be thrown. Its default value is -1, meaning it is disabled.
`hazelcast.rest.enabled` | true | bool |   Enable [REST](#rest-client) client request listener service.
`hazelcast.shutdownhook.enabled` | true | bool  | Enable Hazelcast shutdownhook thread. When this is enabled, this thread terminates the Hazelcast instance without waiting to shutdown gracefully. 
`hazelcast.slow.operation.detector.enabled`|true|bool|Enables/disables the `SlowOperationDetector`.
`hazelcast.slow.operation.detector.log.purge.interval.seconds`|300|int|Purge interval for slow operation logs.
`hazelcast.slow.operation.detector.log.retention.seconds`|3600|int|Defines the retention time of slow operation log invocations. If an invocation is older than this value, it will be purged from the log to prevent unlimited memory usage. When all invocations are purged from a log, the log itself will be deleted.
`hazelcast.slow.operation.detector.stacktrace.logging.enabled`|false|bool|Defines if the stack traces of slow operations are logged in the log file. Stack traces are always reported to the Management Center, but by default, they are not printed to keep the log size small.
`hazelcast.slow.operation.detector.threshold.millis`|10000|int|Defines a threshold above which a running operation in `OperationService` is considered to be slow. These operations log a warning and are shown in the Management Center with detailed information, e.g. stack trace.
`hazelcast.socket.bind.any` | true | bool | Bind both server-socket and client-sockets to any local interface.
`hazelcast.socket.client.bind`|true|bool|Bind client socket to an interface when connecting to a remote server socket. When set to `false`, client socket is not bound to any interface.
`hazelcast.socket.client.bind.any` | true | bool |   Bind client-sockets to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.
`hazelcast.socket.connect.timeout.seconds`|0|int|Socket connection timeout in seconds. `Socket.connect()` will be blocked until either connection is established or connection is refused or this timeout passes. Default is 0, means infinite. 
`hazelcast.socket.keep.alive` | true | bool  | Socket set keep alive (`SO_KEEPALIVE`).
`hazelcast.socket.linger.seconds`|0|int|Set socket `SO_LINGER` option.
`hazelcast.socket.no.delay` | true | bool  |   Socket set TCP no delay.
`hazelcast.socket.receive.buffer.size` | 32 | int | Socket receive buffer (`SO_RCVBUF`) size in KB. If you have a very fast network (e.g. 10gbit) and/or you have large entries, then you may benefit from increasing sender/receiver buffer sizes. Use this property and the next one below tune the size. For example, a send/receive buffer size of 1024 kB is a safe starting point for a 10gbit network.
`hazelcast.socket.send.buffer.size` | 32 | int  | Socket send buffer (`SO_SNDBUF`) size in KB.
`hazelcast.socket.server.bind.any` | true | bool | Bind server-socket to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.
`hazelcast.version.check.enabled` | true | bool  |   Enable Hazelcast new version check on startup.
`hazelcast.wait.seconds.before.join` | 5 | int  | Wait time before join operation.

