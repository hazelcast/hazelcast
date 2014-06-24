# WAN

![](images/enterprise-onlycopy.jpg)




## WAN Replication

There are cases where you would need to synchronize multiple clusters. Synchronization of clusters is named as WAN (Wide Area Network) Replication because it is mainly used for replicating different clusters running on WAN. 

Imagine having different clusters in New York, London and Tokyo. Each cluster would be operating at very high speed in their LAN (Local Area Network) settings but you would want some or all parts of the data in these clusters replicating to each other. So, updates in Tokyo cluster goes to London and NY, in the meantime updates in New York cluster is synchronized to Tokyo and London.

### WAN Replication Configuration
You can setup active-passive WAN Replication where only one active node replicating its updates on the passive one. You can also setup active-active replication where each cluster is actively updating and replication to the other cluster(s).

In the active-active replication setup, there might be cases where each node is updating the same entry in the same named distributed map. Thus, conflicts will occur when merging. For those cases, a conflict resolution will be needed. Below is how you can setup WAN Replication for London cluster for instance.

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

    <network>
    ...
    </network>
...
</hazelcast>
```

This can be the configuration of the cluster running in NY, replicating to Tokyo and London. Tokyo and London clusters should have similar configurations if they are also active replicas.

If NY and London cluster configurations contain `wan-replication` element and Tokyo cluster does not, it means NY and London are active endpoints and Tokyo is passive endpoint.

As noted earlier, you can have Hazelcast replicating some or all of the data in your clusters. You might have 5 different distributed maps but you might want only one of these maps replicating across clusters. So you mark which maps to be replicated by adding `wan-replication-ref` element into map configuration as shown below.

```xml
<hazelcast>
    <wan-replication name="my-wan-cluster">
        ...
    </wan-replication>

    <network>
    ...
    </network>
    <map name="my-shared-map">
        ...
        <wan-replication-ref name="my-wan-cluster">
            <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
        </wan-replication-ref>
    </map>
...
</hazelcast>
```

Here we have `my-shared-map` is configured to replicate itself to the cluster targets defined in the `wan-replication` element.

Note that, you will also need to define a `merge policy` for merging replica entries and resolving conflicts during the merge.

<br> </br>

### WAN Replication Queue Size
For huge clusters or high data mutation rates, it might be necessary to increase the replication queue size. The default queue size for replication queues is `100000`. This means, if
you have heavy put/update/remove rates, you might exceed the queue size so that oldest, not yet replicated, updates might get lost.
 
To increase the replication queue size, Hazelcast Enterprise user can use the `hazelcast.enterprise.wanrep.queuesize` configuration property.

This can either be achieved using a command line property (where xxx is the queue size):

```xml
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
<font color="red">
***Related Information***
</font>

*You can download the white paper *Hazelcast on AWS: Best Practices for Deployment* from [Hazelcast.com](http://hazelcast.com/resources/hazelcast-on-aws-best-practices-for-deployment/).*

<br> </br>

