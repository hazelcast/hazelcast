## Enterprise WAN Replication Configuration

![](images/enterprise-onlycopy.jpg)


The following are example Enterprise WAN replication configurations.

**Declarative Configuration:**

```xml
<wan-replication name="my-wan-cluster">
   <target-cluster group-name="tokyo" group-password="tokyo-pass">
      <replication-impl>com.hazelcast.enterprise.wan.replication.
      WanNoDelayReplication</replication-impl>
      <end-points>
         <address>10.2.1.1:5701</address>
         <address>10.2.1.2:5701</address>
      </end-points> 
   </target-cluster>
</wan-replication>
<wan-replication name="my-wan-cluster-batch" snapshot-enabled="false">
   <target-cluster group-name="london" group-password="london-pass">
      <replication-impl>com.hazelcast.enterprise.wan.replication.
      WanBatchReplication</replication-impl>
      <end-points>
         <address>10.3.5.1:5701</address>
         <address>10.3.5.2:5701</address>
      </end-points>
   </target-cluster>
</wan-replication>
```

**Programmatic Configuration:**

```java
Config config = new Config();

//No delay replication config
WanReplicationConfig wrConfig = new WanReplicationConfig();
WanTargetClusterConfig  wtcConfig = wrConfig.getWanTargetClusterConfig();

wrConfig.setName("my-wan-cluster");
wtcConfig.setGroupName("tokyo").setGroupPassword("tokyo-pass");
wtcConfig.setReplicationImpl("com.hazelcast.enterprise.wan.replication.WanNoDelayReplication");

List<String> endpoints = new ArrayList<String>();
endpoints.add("10.2.1.1:5701");
endpoints.add("10.2.1.1:5701");
wtcConfig.setEndpoints(endpoints);
config.addWanReplicationConfig(wrConfig);

//Batch Replication Config
WanReplicationConfig wrConfig = new WanReplicationConfig();
WanTargetClusterConfig  wtcConfig = wrConfig.getWanTargetClusterConfig();

wrConfig.setName("my-wan-cluster-batch");
wrConfig.setSnapshotEnabled(false);
wtcConfig.setGroupName("london").setGroupPassword("london");
wtcConfig.setReplicationImpl("com.hazelcast.enterprise.wan.replication.WanBatchReplication");

List<String> batchEndpoints = new ArrayList<String>();
batchEndpoints.add("10.3.5.1:5701");
batchEndpoints.add("10.3.5.2:5701");
wtcConfig.setEndpoints(batchEndpoints);
config.addWanReplicationConfig(wrConfig);
```

Enterprise WAN replication configuration has the following elements.

- `name`: Name for your WAN replication configuration.
- `snapshot-enabled`: Only valid when used with `WanBatchReplication`. When set to `true`, only the latest events (based on key) are selected and sent in a batch. 
- `target-cluster`: Configures target cluster's group name and password.
- `replication-impl`: Name of the class implementation for the Enterprise WAN replication.
- `end-points`: IP addresses of the cluster members for which the Enterprise WAN replication is implemented.

### IMap and ICache WAN Configuration

To enable WAN replication for an IMap or ICache instance, you can use `wan-replication-ref` element. 
Each IMap and ICache instance can have different WAN replication configurations.

**Declarative Configuration:**

```xml
<wan-replication name="my-wan-cluster">
   ...
</wan-replication>
<map name="testMap">
 <wan-replication-ref name="testWanRef">
    <merge-policy>TestMergePolicy</merge-policy>
    <republishing-enabled>false</republishing-enabled>
 </wan-replication-ref>
</map>
<cache name="testCache">
   <wan-replication-ref name="testWanRef">
      <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>
   </wan-replication-ref>
</cache>
```

**Programmatic Configuration:**

```java
Config config = new Config();

WanReplicationConfig wrConfig = new WanReplicationConfig();
WanTargetClusterConfig  wtcConfig = wrConfig.getWanTargetClusterConfig();

wrConfig.setName("my-wan-cluster");
...
config.addWanReplicationConfig(wrConfig);

WanReplicationRef wanRef = new WanReplicationRef();
wanRef.setName("my-wan-cluster");
wanRef.setMergePolicy(PassThroughMergePolicy.class.getName());
wanRef.setRepublishingEnabled(true);
config.getMapConfig("testMap").setWanReplicationRef(wanRef);

WanReplicationRef cacheWanRef = new WanReplicationRef();
cacheWanRef.setName("my-wan-cluster");
cacheWanRef.setMergePolicy("com.hazelcast.cache.merge.PassThroughCacheMergePolicy");
cacheWanRef.setRepublishingEnabled(true);
config.getCacheConfig("testCache").setWanReplicationRef(cacheWanRef);
```

![image](images/NoteSmall.jpg) ***NOTE:*** *Caches that are created dynamically do not support WAN replication functionality. Cache configurations should be defined either declaratively (by XML) or programmatically on both source and target clusters.*

`wan-replication-ref` has the following elements;

- `name`: Name of `wan-replication` configuration. IMap or ICache instance uses this `wan-replication` config. Please refer to the [Enterprise WAN Replication Configuration section](#enterprise-wan-replication-configuration) for details about `wan-replication` configuration.
- `merge-policy`: Resolve conflicts that are occurred when target cluster already has the replicated entry key.
- `republishing-enabled`: When enabled, an incoming event to a member is forwarded to target cluster of that member.

**Merge policies:**

4 merge policy implementations for IMap and 2 merge policy implementations for ICache are provided
out of the box.

IMap has the following merge policies:

- `com.hazelcast.map.merge.PutIfAbsentMapMergePolicy`: Incoming entry merges from the source map to the target map if it does not exist in the target map.
- `com.hazelcast.map.merge.HigherHitsMapMergePolicy`: Incoming entry merges from the source map to the target map if the source entry has more hits than the target one.
- `com.hazelcast.map.merge.PassThroughMergePolicy`: Incoming entry merges from the source map to the target map unless the incoming entry is not null.
- `com.hazelcast.map.merge.LatestUpdateMapMergePolicy`: Incoming entry merges from the source map to the target map if the source entry has been updated more recently than the target entry. Please note that this merge policy can only be used when the clusters' clocks are in sync.

ICache has the following merge policies:
 
- `com.hazelcast.cache.merge.HigherHitsCacheMergePolicy`: Incoming entry merges from the source cache to the target cache if the source entry has more hits than the target one.
- `com.hazelcast.cache.merge.PassThroughCacheMergePolicy`: Incoming entry merges from the source cache to the target cache unless the incoming entry is not null.

