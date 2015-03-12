## WAN Replication Configuration

The following are example WAN replication configurations.

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


WAN replication configuration has the following elements.

- name: Name for your WAN replication configuration.
- target-cluster: Creates a group and its password.
- replication-impl: Name of the class implementation for the WAN replication.
- end-points: IP addresses of the cluster members for which the WAN replication is implemented.





