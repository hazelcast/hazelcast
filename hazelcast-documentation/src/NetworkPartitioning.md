
### Network Partitioning - Split Brain Syndrome

Imagine that you have 10-node cluster and for some reason the network is divided into two in a way that 4 servers cannot see the other 6. As a result you ended up having two separate clusters; 4-node cluster and 6-node cluster. Members in each sub-cluster are thinking that the other nodes are dead even though they are not. This situation is called Network Partitioning (a.k.a. Split-Brain Syndrome).

Since it is a network failure, there is no way to avoid it programatically and your application will run as two separate independent clusters. But we should be able to answer the following questions: "What will happen after the network failure is fixed and connectivity is restored between these two clusters? Will these two clusters merge into one again? If they do, how are the data conflicts resolved, because you might end up having two different values for the same key in the same map?"

Here is how Hazelcast deals with it:

1.  The oldest member of the cluster checks if there is another cluster with the same group-name and group-password in the network.

2.  If the oldest member finds such cluster, then it figures out which cluster should merge to the other.

3.  Each member of the merging cluster will do the following:

	-   pause

	-   take locally owned map entries

	-   close all of its network connections (detach from its cluster)

	-   join to the new cluster

	-   send merge request for each of its locally owned map entry

	-   resume

So each member of the merging cluster is actually rejoining to the new cluster and sending merge request for each of its locally owned map entry.

***Question***: Which cluster will merge into the other?

***Answer***: Smaller cluster will merge into the bigger one. If they have equal number of members then a hashing algorithm determines the merging cluster.

***Question***: Each cluster may have different versions of the same key in the same map. How is the conflict resolved?

***Answer***: Destination cluster will decide how to handle merging entry based on the `MergePolicy` set for that map. There are built-in merge policies such as `PassThroughMergePolicy`, `PutIfAbsentMapMergePolicy`, `HigherHitsMapMergePolicy` and `LatestUpdateMapMergePolicy`. But you can develop your own merge policy by implementing `com.hazelcast.map.merge.MapMergePolicy`. You should set the full class name of your implementation to the merge-policy configuration.

```java
public interface MergePolicy {
    /**
    * Returns the value of the entry after the merge
    * of entries with the same key. Returning value can be
    * You should consider the case where existingEntry is null.
    *
    * @param mapName       name of the map
    * @param mergingEntry  entry merging into the destination cluster
    * @param existingEntry existing entry in the destination cluster
    * @return final value of the entry. If returns null then entry will be removed.
    */
    Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry);
}
```

Here is how merge policies are specified per map:

```xml
<hazelcast>
    ...
    <map name="default">
        <backup-count>1</backup-count>
        <eviction-policy>NONE</eviction-policy>
        <max-size>0</max-size>
        <eviction-percentage>25</eviction-percentage>
        <!--
            While recovering from split-brain (network partitioning),
            map entries in the small cluster will merge into the bigger cluster
            based on the policy set here. When an entry merge into the
            cluster, there might an existing entry with the same key already.
            Values of these entries might be different for that same key.
            Which value should be set for the key? Conflict is resolved by
            the policy set here. Default policy is hz.ADD_NEW_ENTRY

            There are built-in merge policies such as
            There are built-in merge policies such as
            com.hazelcast.map.merge.PassThroughMergePolicy; entry will be added if there is no existing entry for the key.
            com.hazelcast.map.merge.PutIfAbsentMapMergePolicy ; entry will be added if the merging entry doesn't exist in the cluster.
            com.hazelcast.map.merge.HigherHitsMapMergePolicy ; entry with the higher hits wins.
            com.hazelcast.map.merge.LatestUpdateMapMergePolicy ; entry with the latest update wins.
        -->
        <merge-policy>MY_MERGE_POLICY_CLASS</merge-policy>
    </map>

    ...
</hazelcast>
```
