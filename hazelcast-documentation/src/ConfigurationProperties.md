## Advanced Configuration Properties

There are some advanced configuration properties to tune some aspects of Hazelcast. These can be set as property name and value pairs through configuration xml, configuration API or JVM system property.

-   **Configuration xml**

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
-   **Configuration API**

```java
Config cfg = new Config() ;
cfg.setProperty("hazelcast.property.foo", "value");
```
-   **System Property**

1.  Using JVM parameter: `java -Dhazelcast.property.foo=value`

2.  Using System class: `System.setProperty("hazelcast.property.foo", "value");`

- **Property Name | default value | type | description**

- `hazelcast.memcache.enabled | true | bool` |   Enable [Memcache](#memcache-client) client request listener service

- `hazelcast.rest.enabled | true | bool` |   Enable [REST](#rest-client) client request listener service

- `hazelcast.logging.type | jdk | enum` |   Name of [logging](#logging-configuration) framework type to send logging events.
 
- `hazelcast.map.load.chunk.size | 1000 | int` |   Chunk size for [MapLoader](#persistence) 's map initialization process (MapLoder.loadAllKeys())

- `hazelcast.merge.first.run.delay.seconds | 300 | int` |   Inital run delay of [split brain/merge process](#network-partitioning-split-brain-syndrome) in seconds

- `hazelcast.merge.next.run.delay.seconds | 120 | int` |   Run interval of [split brain/merge process](#network-partitioning-split-brain-syndrome) in seconds
 
- `hazelcast.socket.bind.any | true | bool` |   Bind both server-socket and client-sockets to any local interface

- `hazelcast.socket.server.bind.any | true | bool ` |   Bind server-socket to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.

- `hazelcast.socket.client.bind.any | true | bool ` |   Bind client-sockets to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.

- `hazelcast.socket.receive.buffer.size | 32 | int ` |   Socket receive buffer size in KB

- `hazelcast.socket.send.buffer.size | 32 | int ` |   Socket send buffer size in KB

- `hazelcast.socket.keep.alive | true | bool ` |   Socket set keep alive

- `hazelcast.socket.no.delay | true | bool ` |   Socket set TCP no delay

- `hazelcast.prefer.ipv4.stack | true | bool ` |   Prefer Ipv4 network interface when picking a local address.

- `hazelcast.shutdownhook.enabled | true | bool ` |   Enable Hazelcast shutdownhook thread

- `hazelcast.wait.seconds.before.join | 5 | int ` |   Wait time before join operation

- `hazelcast.max.wait.seconds.before.join | 20 | int ` |   Maximum wait time before join operation

- `hazelcast.heartbeat.interval.seconds | 1 | int ` |   Heartbeat send interval in seconds

- `hazelcast.max.no.heartbeat.seconds | 300 | int ` |   Max timeout of heartbeat in seconds for a node to assume it is dead

- `hazelcast.icmp.enabled | false | bool ` |   Enable ICMP ping

- `hazelcast.icmp.timeout | 1000 | int ` |   ICMP timeout in ms

- `hazelcast.icmp.ttl | 0 | int ` |   ICMP TTL | maximum numbers of hops to try)

- `hazelcast.master.confirmation.interval.seconds | 30 | int ` |   Interval at which nodes send master confirmation

- `hazelcast.max.no.master.confirmation.seconds | 450 | int ` |   Max timeout of master confirmation from other nodes

- `hazelcast.member.list.publish.interval.seconds | 600 | int ` |   Interval at which master node publishes a member list

- `hazelcast.prefer.ipv4.stack | true | bool ` |   Prefer IPv4 Stack, don't use IPv6. See [IPv6 doc.](#ipv6-support)

- `hazelcast.initial.min.cluster.size | 0 | int ` |   Initial expected cluster size to wait before node to start completely

- `hazelcast.initial.wait.seconds | 0 | int ` |   Inital time in seconds to wait before node to start completely

- `hazelcast.partition.count | 271 | int ` |   Total partition count

- `hazelcast.jmx | false | bool ` |   Enable [JMX](#monitoring-with-jmx) agent

- `hazelcast.jmx.detailed | false | bool ` |   Enable detailed views on [JMX](#monitoring-with-jmx)

- `hazelcast.mc.map.excludes | null | CSV ` |   Comma seperated map names to exclude from [Hazelcast Management Center](http://www.hazelcast.com/mancenter.jsp)

- `hazelcast.mc.queue.excludes | null | CSV ` |   Comma seperated queue names to exclude from [Hazelcast Management Center](http://www.hazelcast.com/mancenter.jsp)

- `hazelcast.mc.topic.excludes | null | CSV ` |   Comma seperated topic names to exclude from [Hazelcast Management Center](http://www.hazelcast.com/mancenter.jsp)

- `hazelcast.version.check.enabled | true | bool ` |   Enable Hazelcast new version check on startup

- `hazelcast.mc.max.visible.instance.count | 100 | int ` |   Management Center maximum visible instance count

- `hazelcast.connection.monitor.interval | 100 | int ` |   Minimum interval to consider a connection error as critical in milliseconds.

- `hazelcast.connection.monitor.max.faults | 3 | int ` |   Maximum IO error count before disconnecting from a node.

- `hazelcast.partition.migration.interval | 0 | int ` |   Interval to run partition migration tasks in seconds.

- `hazelcast.partition.migration.timeout | 300 | int ` |   Timeout for partition migration tasks in seconds.

- `hazelcast.graceful.shutdown.max.wait | 600 | int ` |   Maximum wait seconds during graceful shutdown.

- `hazelcast.mc.url.change.enabled | true | bool ` |   Management Center changing server url is enabled

- `hazelcast.elastic.memory.enabled | false | bool ` |   Enable [Hazelcast Elastic Memory](#elastic-memory-enterprise-edition-only) off-heap storage

- `hazelcast.elastic.memory.total.size | 128 | int ` |   [Hazelcast Elastic Memory](#elastic-memory-enterprise-edition-only) storage total size in MB

- `hazelcast.elastic.memory.chunk.size | 1 | int ` |   [Hazelcast Elastic Memory](#elastic-memory-enterprise-edition-only) storage chunk size in KB

- `hazelcast.elastic.memory.shared.storage | false | bool ` |   [Enable Hazelcast Elastic Memory](#elastic-memory-enterprise-edition-only) shared storage

- `hazelcast.enterprise.license.key | null | string ` |   [Hazelcast Enterprise](http://www.hazelcast.com/products.jsp) license key

- `hazelcast.system.log.enabled | true | bool ` |   Enable system logs
