# WAN

## WAN Replication

There are cases where you need to synchronize multiple clusters to the same state. Synchronization of clusters, also known as
WAN (Wide Area Network) Replication, is mainly used for replicating stats of different clusters over WAN environments like
the Internet. 

Imagine you have different data centers in New York, London and Tokyo each running an independent Hazelcast cluster. Every cluster
would be operating at native speed in their own LAN (Local Area Network) settings but you also want some or all recordsets in
these clusters to be replicated to each other: updates to Tokyo cluster also go to London and New York, in the meantime updates
from New York cluster are synchronized to Tokyo and London.

### Configuring WAN Replication

The current WAN Replication implementation supports two different operation modes.

- **Active-Passive:** This mode is mostly used for failover scenarios where you want to replicate only one active cluster to one
  or more non-active ones for backup reasons.

- **Active-Active:** Every cluster is fully equal and all clusters replicate to all others. This is normally used to connect
  different clients to different clusters for the sake of the shortest path between client and server.

Let's see how we can set up WAN Replication for London and Tokyo clusters:

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

By using an Active-Active Replication setup, you might end up in situations where multiple clusters simultaneously update the same
entry in the same distributed data structure. Those situations will cause conflicts, which makes it sufficient for you to provide
merge-policies to resolve those conflicts. 

```xml
<hazelcast>
  <wan-replication name="my-wan-cluster">
    <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
    ...
  </wan-replication>
  ...
</hazelcast>
```

As noted earlier, you can have Hazelcast replicating only some or all of the data in your cluster. Imagine you have 5 different
distributed maps but you might want only one of these maps replicating across clusters. To achieve this, you mark the maps to be
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

`WanNoDelayReplication` sends replication events to the target cluster as soon as they are generated. As its name suggests,
`WanBatchReplication` waits until:

-  a pre-defined number of replication events are generated, (please refer to the [Wan Replication Batch Size section](#wan-replication-batch-size)).
- or a pre-defined amount of time is passed (please refer to the [Wan Replication Batch Frequency section](#wan-replication-batch-frequency)).

### WAN Replication Batch Size

When `WanBatchReplication` is preferred as the replication implementation, the maximum size of events that are sent in a single batch can be changed 
depending on your needs. Default value for batch size is `50`.

To change the `WanBatchReplication` batch size, the Hazelcast Enterprise user can use the `hazelcast.enterprise.wanrep.batch.size`
configuration element.

You can do this by setting the property on the command line (where xxx is the batch size),

```plain
-Dhazelcast.enterprise.wanrep.batch.size=xxx
```

or by setting the properties inside the `hazelcast.xml` (where xxx is the requested batch size):

```xml
<hazelcast>
  <properties>
    <property name="hazelcast.enterprise.wanrep.batch.size">xxx</property>
  </properties>
</hazelcast>
``` 

### WAN Replication Batch Frequency

When `WanBatchReplication`is preferred as the replication implementation and the number of generated WAN replication events does not reach [Wan Replication Batch Size](#wan-replication-batch-size),
they are sent to the target cluster after a certain amount of time is passed.

Default value of for this duration is `5` seconds.

To change the `WanBatchReplication` batch sending frequency, the Hazelcast Enterprise user can use the `hazelcast.enterprise.wanrep.batchfrequency.seconds`
configuration element.

You can do this by setting the property on the command line (where xxx is the batch sending frequency in seconds),

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

After a replication event is sent to the target cluster, target member waits for an acknowledge to be sure that event is reached to target.
If confirmation is not received in the period of timeout duration, event is resent to the target cluster.

Default value of for this duration is `5000` milliseconds.

You can change this duration depending on your network latency. The Hazelcast Enterprise user can use the `hazelcast.enterprise.wanrep.optimeout.millis`
configuration element to change the timeout duration.

You can do this by setting the property on the command line (where xxx is the timeout duration in milliseconds),

```plain
-Dhazelcast.enterprise.wanrep.optimeout.millis=xxx
```

or by setting the properties inside the `hazelcast.xml` (where xxx is the requested timeout duration):

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
 
To increase the replication queue capacity, the Hazelcast Enterprise user can use the `hazelcast.enterprise.wanrep.queue.capacity`
configuration element.

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

*Please refer to the [WAN Replication Configuration section](#wan-replication-configuration) for a full description of Hazelcast WAN Replication configuration.*



