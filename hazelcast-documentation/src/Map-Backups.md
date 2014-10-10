


### Map Backups


Hazelcast will distribute map entries onto multiple JVMs (cluster members). Each JVM holds some portion of the data but you do not want to lose data when a JVM crashes.
 
Distributed maps have 1 backup by default so that if a member goes down, you do not lose data. Backup operations are synchronous, so when a `map.put(key, value)` returns, it is guaranteed that the entry is replicated to one other node. For the reads, it is also guaranteed that `map.get(key)` returns the latest value of the entry. Consistency is strictly enforced.


#### Sync Backup

To provide data safety, Hazelcast allows you to specify the number of backup copies you want to have. That way, data on a JVM will be copied onto other JVM(s). It is configured using the `backup-count` property.

```xml
<hazelcast>
  <map name="default">
    <backup-count>1</backup-count>
  </map>
</hazelcast>
```

When this count is 1, it means that a map entry will have its backup on another node in the cluster. If it is set as 2, then it will have its backup on two other nodes. It can be set as 0, if you do not want your entries to be backed up, e.g. if performance is more important than backing up. Maximum value for this property is 6.

Hazelcast supports both synchronous and asynchronous backups. By default, backup operations are synchronous (configured with `backup-count`). In this case, backup operations block operations until backups are successfully copied to backups nodes (or deleted from backup nodes in case of remove) and acknowledgements are received. Therefore, for example, you can be sure that backups are updated before a `put` operation is completed. Of course, sync backup operations have a blocking cost which may lead to latency issues.

#### Async Backup

Asynchronous backups, on the other hand, do not block operations. They are fire & forget and do not require acknowledgements (backup operations are performed at some point in time). Async backup is configured using the `async-backup-count` property.
 

```xml
<hazelcast>
  <map name="default">
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
  </map>
</hazelcast>
```

<br></br>
***ATTENTION:*** *Backups increase memory usage since they are also kept in memory. So for every backup, you  double the original memory consumption.*

![image](images/NoteSmall.jpg) ***NOTE:*** *A map can have both sync and aysnc backups at the same time.*



#### Read Backup Data

By default, Hazelcast will have one sync backup copy. If backup count is more than 1, then each member will carry both owned entries and backup copies of other member(s). So for the `map.get(key)` call, it is possible that calling member has backup copy of that key but by default, `map.get(key)` will always read the value from the actual owner of the key for consistency.
It is possible to enable backup reads (read local backup entries) by setting the value of `read-backup-data` property to **true**. Its default value is **false** for strong consistency. Enabling backup reads can improve the performance. 

```xml
<hazelcast>
  <map name="default">
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
    <read-backup-data>true</read-backup-data>
  </map>
</hazelcast>
```

This feature is available when there is at least 1 sync or async backup.
