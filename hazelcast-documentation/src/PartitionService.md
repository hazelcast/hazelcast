
### Cluster-Member Safety Check

To prevent data loss when shutting down a node, Hazelcast provides a graceful shutdown feature. You perform this by calling the method `HazelcastInstance.shutdown()`. Once this method is called, it checks the following conditions to ensure the node is safe to shutdown.

- There is no active migration.
- At least one backup of partitions are synced with primary ones.

Even if the above conditions are not met, `HazelcastInstance.shutdown()` will force them to be completed. Eventually, when this method returns, it means the node has been brought to a safe state and it can be shut down without any data loss. 

What if you want to be sure that your **cluster** is in a safe state? What does it mean that cluster is safe to shutdown without any data loss? 

There may be some use cases like rolling upgrades, development/testing or any logic that require a cluster/member to be safe. To provide this, Hazelcast offers the `PartitionService` interface with the methods `isClusterSafe`, `isMemberSafe`, `isLocalMemberSafe` and `forceLocalMemberToBeSafe`. These methods can be deemed as decoupled pieces from the method `Hazelcast.shutdown`. 


```java
public interface PartitionService {
   ...
   ...
    boolean isClusterSafe();
    boolean isMemberSafe(Member member);
    boolean isLocalMemberSafe();
    boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit);
}
```

The method `isClusterSafe` checks whether the cluster is in a safe state. It returns `true` if there are no active partition migrations and there are sufficient backups for each partition. Once it returns `true`, the cluster is safe and a node can be shut down without data loss.

The method `isMemberSafe` checks whether a specific node is in a safe state. This check controls if the first backups of partitions of the given node are synced with the primary ones. Once it returns `true`, the given node is safe and it can be shut down without data loss. Similarly, the method `isLocalMemberSafe` does the same check for the local member. The method `forceLocalMemberToBeSafe` forces the owned and backup partitions to be synchronized, making the local member safe.

![image](images/NoteSmall.jpg) ***NOTE:*** *These methods are available from Hazelcast 3.3.*


#### Sample Codes


```java
PartitionService partitionService = hazelcastInstance.getPartitionService().isClusterSafe()
if (partitionService().isClusterSafe()) {
  hazelcastInstance.shutdown(); // or terminate
}
```

OR 

```java
PartitionService partitionService = hazelcastInstance.getPartitionService().isClusterSafe()
if (partitionService().isLocalMemberSafe()) {
  hazelcastInstance.shutdown(); // or terminate
}
```
<br></br>

***RELATED INFORMATION***

*For more code samples please refer to [PartitionService Code Samples](https://github.com/hazelcast/hazelcast-code-samples/tree/master/monitoring/cluster/src/main/java)*.
