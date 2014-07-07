# WAN

![](images/enterprise-onlycopy.jpg)

## WAN Replication

There are cases where you need to synchronize multiple clusters to the same state. Synchronization of clusters, also known as
WAN (Wide Area Network) Replication , it is mainly used for replicating stats of different clusters over WAN environments like
the Internet. 

Imagine you have different datacenters in New York, London and Tokyo each running an independent Hazelcast cluster. Every cluster
would be operating at native speed in their own LAN (Local Area Network) settings but you also want some or all recordsets in
these clusters replicated to each other. So, updates to Tokyo cluster also go to London and New York, in the meantime updates
from New York cluster are synchronized to Tokyo and London.

### WAN Replication Configuration

The current WAN Replication implementation supports two different operation modes: 

- **Active-Passive:** This mode is mostly used for failover scenarios where you want to replicate only one active cluster to one
  or more non active ones for backup reasons

- **Active-Active:** Every cluster is fully equal and all clusters replicating to all others which is normally used to connect
  different clients to different clusters for the sake of shortest path between client and server.

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

Using this configuration, the cluster running in NY is replicating to Tokyo and London. Tokyo and London clusters should
have a similar configurations if you want to run in Active-Active mode.

If New York and London cluster configurations contain the `wan-replication` element and Tokyo cluster does not, it means
New York and London are active endpoints and Tokyo is a passive endpoint.

By using an Active-Active Replication setup, you might end up in situations where multiple clusters simultaneously updating the same
entry in the same distributed data structure. Those situations will cause conflicts which make it sufficient to provide
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
distributed maps but you might want only one of these maps replicating across clusters. To achieve this you mark the maps to be
replicated by adding `wan-replication-ref` element in the map configuration as shown below.

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

Note that, you will also have to define a `merge policy` for merging replica entries and resolving conflicts during the merge
as mentioned before.

### WAN Replication Queue Size
For huge clusters or high data mutation rates, it might be necessary to increase the replication queue size. The default queue
size for replication queues is `100000`. This means, if you have heavy put/update/remove rates, you might exceed the queue size
so that oldest, not yet replicated, updates might get lost.
 
To increase the replication queue size, Hazelcast Enterprise user can use the `hazelcast.enterprise.wanrep.queuesize`
configuration property.

This can either be achieved using a command line property (where xxx is the queue size):

```plain
-Dhazelcast.enterprise.wanrep.queuesize=xxx
```

or using properties inside the `hazelcast.xml` (also here change xxx to the requested queue size):

```xml
<hazelcast>
  <properties>
    <property name="hazelcast.enterprise.wanrep.queuesize">xxx</property>
  </properties>
</hazelcast>
```

### WAN Replication Additional Information

***RELATED INFORMATION***

_You can download the white paper **Hazelcast on AWS: Best Practices for Deployment** from
[Hazelcast.com](http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/)._
