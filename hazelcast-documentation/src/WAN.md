# WAN

## WAN Replication

There are cases where you need to synchronize multiple clusters to the same state. Synchronization of clusters, also known as
WAN (Wide Area Network) Replication, is mainly used for replicating state of different clusters over WAN environments like
the Internet. 

Imagine you have different data centers in New York, London and Tokyo each running an independent Hazelcast cluster. Every cluster
would be operating at native speed in their own LAN (Local Area Network), but you also want some or all recordsets in
these clusters to be replicated to each other: updates in the Tokyo cluster should also replicate to London and New York, in the meantime updates
in the New York cluster are synchronized to the Tokyo and London clusters.

### Configuring WAN Replication

The current WAN Replication implementation supports two different operation modes.

- **Active-Passive:** This mode is mostly used for failover scenarios where you want to replicate an active cluster to one
  or more passive clusters, for the purpose of maintaining a backup.

- **Active-Active:** Every cluster is equal, each cluster replicate to all other clusters. This is normally used to connect
  different clients to different clusters for the sake of the shortest path between client and server.

Let's see how we can configure WAN Replication from the New York cluster to target the London and Tokyo clusters:

```xml
<hazelcast>
  <wan-replication name="my-wan-cluster">
    <target-cluster group-name="tokyo" group-password="tokyo-pass">
      <replication-impl>com.hazelcast.wan.impl.WanNoDelayReplication</replication-impl>
      <end-points>
        <address>10.2.1.1:5701</address>
        <address>10.2.1.2:5701</address>
      </end-points>
    </target-cluster>
    <target-cluster group-name="london" group-password="london-pass">
      <replication-impl>com.hazelcast.wan.impl.WanNoDelayReplication</replication-impl>
      <end-points>
        <address>10.3.5.1:5701</address>
        <address>10.3.5.2:5701</address>
      </end-points>
    </target-cluster>
  </wan-replication>
  ...
</hazelcast>
```

Using this configuration, the cluster running in New York is replicating to Tokyo and London. The Tokyo and London clusters should
have a similar configurations if you want to run in Active-Active mode.

If the New York and London cluster configurations contain the `wan-replication` element and the Tokyo cluster does not, it means
New York and London are active endpoints and Tokyo is a passive endpoint.

When using Active-Active Replication, multiple clusters can simultaneously update the same entry in a distributed data structure.
You can configure a merge-policy to resolve these potential conflicts.

```xml
<hazelcast>
  <wan-replication name="my-wan-cluster">
    <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
    ...
  </wan-replication>
  ...
</hazelcast>
```

Hazelcast can configure WAN replication on a per Map basis. Imagine you have different distributed maps, however only one map should be replicated to a target cluster. To achieve this, configure map to be
replicated by adding the `wan-replication-ref` element in the map configuration as shown below.

```xml
<hazelcast>
  <wan-replication name="my-wan-cluster">
    ...
  </wan-replication>
  <map name="my-shared-map">
    <wan-replication-ref name="my-wan-cluster">
    <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
      ...
    </wan-replication-ref>
  </map>
  ...
</hazelcast>
```

You see that we have `my-shared-map` configured to replicate itself to the cluster targets defined in the earlier
`wan-replication` element.

You will also have to define a `merge policy` for merging replica entries and resolving conflicts during the merge
as mentioned before.

### WAN Replication Additional Information

***RELATED INFORMATION***

_You can download the white paper **Hazelcast on AWS: Best Practices for Deployment** from
[Hazelcast.com](http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/)._

<br></br>

***RELATED INFORMATION***


*Please refer to the [WAN Replication Configuration section](#wan-replication-configuration) for a full description of Hazelcast WAN Replication configuration.*

## Enterprise WAN Replication

![](images/enterprise-onlycopy.jpg)

### Replication implementations

Enterprise WAN replication has two different replication implementations. These are `WanNoDelayReplication` and `WanBatchReplication` implementations.
You can configure them using the configuration element `replication-impl`, as shown below.

```xml
<hazelcast>
  <wan-replication name="my-wan-cluster">
    <target-cluster group-name="tokyo" group-password="tokyo-pass">
      <replication-impl>com.hazelcast.enterprise.wan.replication.WanNoDelayReplication</replication-impl>
      ...
    </target-cluster>
  </wan-replication>
</hazelcast>
```

```xml
<hazelcast>
  <wan-replication name="my-wan-cluster">
    <target-cluster group-name="tokyo" group-password="tokyo-pass">
      <replication-impl>com.hazelcast.enterprise.wan.replication.WanBatchReplication</replication-impl>
      ...
    </target-cluster>
  </wan-replication>
</hazelcast>
```

`WanNoDelayReplication` sends replication events to the target cluster as soon as they are generated.
`WanBatchReplication` waits until:

-  a pre-defined number of replication events are generated, (please refer to the [Wan Replication Batch Size section](#wan-replication-batch-size)).
- or a pre-defined amount of time is passed (please refer to the [Wan Replication Batch Frequency section](#wan-replication-batch-frequency)).

### WAN Replication Batch Size

When `WanBatchReplication` is preferred as the replication implementation, the maximum size of events that are sent in a single batch can be changed 
depending on your needs. Default value for batch size is `50`.

To change the `WanBatchReplication` batch size, use the `hazelcast.enterprise.wanrep.batch.size` property in Hazelcast Enterprise.

You can do this by setting the property on the command line (where xxx is the batch size),

```plain
-Dhazelcast.enterprise.wanrep.batch.size=xxx
```

or by setting the property inside the `hazelcast.xml` (where xxx is the requested batch size):

```xml
<hazelcast>
  <properties>
    <property name="hazelcast.enterprise.wanrep.batch.size">xxx</property>
  </properties>
</hazelcast>
``` 

### WAN Replication Batch Frequency

When using `WanBatchReplication` if the number of WAN replication events generated does not reach [Wan Replication Batch Size](#wan-replication-batch-size),
they are sent to the target cluster after a certain amount of time is passed.

Default value of for this duration is `5` seconds.

To change the `WanBatchReplication` batch sending frequency, set `hazelcast.enterprise.wanrep.
batchfrequency.seconds` property.

You can set the property on the command line (where xxx is the batch sending frequency in seconds),

```plain
-Dhazelcast.enterprise.wanrep.batchfrequency.seconds=xxx
```

or by setting the properties inside the `hazelcast.xml` (where xxx is the requested batch sending frequency):

```xml
<hazelcast>
  <properties>
    <property name="hazelcast.enterprise.wanrep.batchfrequency.seconds">xxx</property>
  </properties>
</hazelcast>
``` 

### WAN Replication Operation Timeout

After a replication event is sent to the target cluster, the source member waits for an acknowledge that event has reached the target.
If confirmation is not received inside a timeout duration window, the event is resent to the target cluster.

Default value of for this duration is `5000` milliseconds.

You can change this duration depending on your network latency. The Hazelcast Enterprise user can set the `hazelcast.enterprise.wanrep.optimeout.millis`
property to change the timeout duration.

You can do this by setting the property on the command line (where xxx is the timeout duration in milliseconds),

```plain
-Dhazelcast.enterprise.wanrep.optimeout.millis=xxx
```

or by setting the property inside the `hazelcast.xml` (where xxx is the requested timeout duration):

```xml
<hazelcast>
  <properties>
    <property name="hazelcast.enterprise.wanrep.optimeout.millis">xxx</property>
  </properties>
</hazelcast>
``` 

### WAN Replication Queue Capacity

For huge clusters or high data mutation rates, you might need to increase the replication queue size. The default queue
size for replication queues is `100000`. This means, if you have heavy put/update/remove rates, you might exceed the queue size
so that the oldest, not yet replicated, updates might get lost.
 
To increase the replication queue capacity, the Hazelcast Enterprise user can use the `hazelcast.enterprise.
wanrep.queue.capacity`
property.

You can do this by setting the property on the command line (where xxx is the queue size),

```plain
-Dhazelcast.enterprise.wanrep.queue.capacity=xxx
```

or by setting the properties inside the `hazelcast.xml` (where xxx is the requested queue size):

```xml
<hazelcast>
  <properties>
    <property name="hazelcast.enterprise.wanrep.queue.capacity">xxx</property>
  </properties>
</hazelcast>
```

### Enterprise WAN Replication Additional Information

Each cluster in WAN topology has to have a unique `group-name` property for a proper handling of forwarded events. 

*Please refer to the [Enterprise WAN Replication Configuration section](#enterprise-wan-replication-configuration) for a full description of Hazelcast WAN Replication configuration.*



