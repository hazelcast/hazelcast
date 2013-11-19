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

|Property Name|Description|Value Type|Default|
|-------------|-----------|----------|-------|
|`hazelcast.memcache.enabled`|Enable [Memcache](#MemcacheClient) client request listener service|boolean|true|
|`hazelcast.rest.enabled`|Enable [REST](#RestClient) client request listener service|boolean|true|
|`hazelcast.logging.type`|Name of [logging](#Logging) framework type to send logging events.|enum|jdk|
|`hazelcast.map.load.chunk.size`|Chunk size for [MapLoader](#MapPersistence) 's map initialization process (MapLoder.loadAllKeys())|integer|1000|
|`hazelcast.merge.first.run.delay.seconds`|Inital run delay of [split brain/merge process](#NetworkPartitioning) in seconds|integer|300|
|`hazelcast.merge.next.run.delay.seconds`|Run interval of [split brain/merge process](#NetworkPartitioning) in seconds|integer|120|
|`hazelcast.socket.bind.any`|Bind both server-socket and client-sockets to any local interface|boolean|true|
|`hazelcast.socket.server.bind.any`|Bind server-socket to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.|boolean|true|
|`hazelcast.socket.client.bind.any`|Bind client-sockets to any local interface. If not set, `hazelcast.socket.bind.any` will be used as default.|boolean|true|
|`hazelcast.socket.receive.buffer.size`|Socket receive buffer size in KB|integer|32|
|`hazelcast.socket.send.buffer.size`|Socket send buffer size in KB|integer|32|
|`hazelcast.socket.keep.alive`|Socket set keep alive|boolean|true|
|`hazelcast.socket.no.delay`|Socket set TCP no delay|boolean|true|
|`hazelcast.prefer.ipv4.stack`|Prefer Ipv4 network interface when picking a local address.|boolean|true|
|`hazelcast.shutdownhook.enabled`|Enable Hazelcast shutdownhook thread|boolean|true|
|`hazelcast.wait.seconds.before.join`|Wait time before join operation|integer|5|
|`hazelcast.max.wait.seconds.before.join`|Maximum wait time before join operation|integer|20|
|`hazelcast.heartbeat.interval.seconds`|Heartbeat send interval in seconds|integer|1|
|`hazelcast.max.no.heartbeat.seconds`|Max timeout of heartbeat in seconds for a node to assume it is dead|integer|300|
|`hazelcast.icmp.enabled`|Enable ICMP ping|boolean|false|
|`hazelcast.icmp.timeout`|ICMP timeout in ms|int|1000|
|`hazelcast.icmp.ttl`|ICMP TTL (maximum numbers of hops to try)|int|0|
|`hazelcast.master.confirmation.interval.seconds`|Interval at which nodes send master confirmation|integer|30|
|`hazelcast.max.no.master.confirmation.seconds`|Max timeout of master confirmation from other nodes|integer|450|
|`hazelcast.member.list.publish.interval.seconds`|Interval at which master node publishes a member list|integer|600|
|`hazelcast.prefer.ipv4.stack`|Prefer IPv4 Stack, don't use IPv6. See [IPv6 doc.](#IPv6)|boolean|true|
|`hazelcast.initial.min.cluster.size`|Initial expected cluster size to wait before node to start completely|integer|0|
|`hazelcast.initial.wait.seconds`|Inital time in seconds to wait before node to start completely|integer|0|
|`hazelcast.partition.count`|Total partition count|integer|271|
|`hazelcast.jmx`|Enable [JMX](#JMX) agent|boolean|false|
|`hazelcast.jmx.detailed`|Enable detailed views on [JMX](#JMX)|boolean|false|
|`hazelcast.mc.map.excludes`|Comma seperated map names to exclude from [Hazelcast Management Center](http://www.hazelcast.com/mancenter.jsp)|CSV|null|
|`hazelcast.mc.queue.excludes`|Comma seperated queue names to exclude from [Hazelcast Management Center](http://www.hazelcast.com/mancenter.jsp)|CSV|null|
|`hazelcast.mc.topic.excludes`|Comma seperated topic names to exclude from [Hazelcast Management Center](http://www.hazelcast.com/mancenter.jsp)|CSV|null|
|`hazelcast.version.check.enabled`|Enable Hazelcast new version check on startup|boolean|true|
|`hazelcast.mc.max.visible.instance.count`|Management Center maximum visible instance count|integer|100|
|`hazelcast.connection.monitor.interval`|Minimum interval to consider a connection error as critical in milliseconds.|integer|100|
|`hazelcast.connection.monitor.max.faults`|Maximum IO error count before disconnecting from a node.|integer|3|
|`hazelcast.partition.migration.interval`|Interval to run partition migration tasks in seconds.|integer|0|
|`hazelcast.partition.migration.timeout`|Timeout for partition migration tasks in seconds.|integer|300|
|`hazelcast.graceful.shutdown.max.wait`|Maximum wait seconds during graceful shutdown.|integer|600|
|`hazelcast.mc.url.change.enabled`|Management Center changing server url is enabled|boolean|true|
|`hazelcast.elastic.memory.enabled`|Enable [Hazelcast Elastic Memory](#ElasticMemory) off-heap storage|boolean|false|
|`hazelcast.elastic.memory.total.size`|[Hazelcast Elastic Memory](#ElasticMemory) storage total size in MB|integer|128|
|`hazelcast.elastic.memory.chunk.size`|[Hazelcast Elastic Memory](#ElasticMemory) storage chunk size in KB|integer|1|
|`hazelcast.elastic.memory.shared.storage`|[Enable Hazelcast Elastic Memory](#ElasticMemory) shared storage|boolean|false|
|`hazelcast.enterprise.license.key`|[Hazelcast Enterprise](http://www.hazelcast.com/products.jsp) license key|string|null|
|`hazelcast.system.log.enabled`|Enable system logs|boolean|true|


