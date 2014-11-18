### WAN Replication Configuration

**Declarative Configuration:**

```xml
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
```

**Programmatic Configuration:**

```java
Config config = new Config();
WanReplicationConfig wrConfig = config.getWanReplicationConfig();
WanTargetClusterConfig  wtcConfig = wrConfig.getWanTargetClusterConfig();

wrConfig.setName("my-wan-cluster");
wtcConfig.setGroupName("tokyo").setGroupPassword("tokyo-pass");
wtcConfig.setReplicationImplObject("com.hazelcast.wan.impl.WanNoDelayReplication");
```


It has below attributes.

- name: Name for your WAN replication configuration.
- target-cluster: Create a group and its password using this parameter.
- replication-impl: ???
- end-points: ???





