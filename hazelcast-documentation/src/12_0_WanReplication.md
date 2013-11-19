

# WAN Replication

There are cases where you would need to synchronize multiple clusters. Synchronization of clusters is named as WAN (Wide Area Network) Replication because it is mainly used for replicating different clusters running on WAN. Imagine having different clusters in New York, London and Tokyo. Each cluster would be operating at very high speed in their LAN (Local Area Network) settings but you would want some or all parts of the data in these clusters replicating to each other. So updates in Tokyo cluster goes to London and NY, in the meantime updates in New York cluster is synchronized to Tokyo and London.

You can setup active-passive WAN Replication where only one active node replicating its updates on the passive one. You can also setup active-active replication where each cluster is actively updating and replication to the other cluster(s).

In the active-active replication setup, there might be cases where each node is updating the same entry in the same named distributed map. Thus, conflicts will occur when merging. For those cases, conflict-resolution will be needed. Here is how you can setup WAN Replication for London cluster for instance:

```xml
<hazelcast>
    <wan-replication name="my-wan-cluster">
        <target-cluster group-name="tokyo" group-password="tokyo-pass">
            <replication-impl>com.hazelcast.wan.WanNoDelayReplication</replication-impl>
            <end-points>
                <address>10.2.1.1:5701</address>
                <address>10.2.1.2:5701</address>
            </end-points>
        </target-cluster>
        <target-cluster group-name="london" group-password="london-pass">
            <replication-impl>com.hazelcast.wan.wan.WanNoDelayReplication</replication-impl>
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

If NY and London cluster configurations contain `wan-replication` element and Tokyo cluster doesn't then it means NY and London are active endpoints and Tokyo is passive endpoint.

As noted earlier you can have Hazelcast replicate some or all of the data in your clusters. You might have 5 different distributed maps but you might want only one of these maps replicating across clusters. So you mark which maps to replicate by adding `wan-replication-ref` element into map configuration.

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

Note that you will also need to define a `merge policy` for merging replica entries and resolving conflicts during the merge.
