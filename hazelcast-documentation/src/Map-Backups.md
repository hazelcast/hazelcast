


### Map Backups


Hazelcast distributes map entries onto multiple JVMs (cluster members). Each JVM holds some portion of the data.
 
Distributed maps have 1 backup by default. If a member goes down, you do not lose data. Backup operations are synchronous, so when a `map.put(key, value)` returns, it is guaranteed that the entry is replicated to one other node. For the reads, it is also guaranteed that `map.get(key)` returns the latest value of the entry. Consistency is strictly enforced.


#### Sync Backup

To provide data safety, Hazelcast allows you to specify the number of backup copies you want to have. That way, data on a JVM will be copied onto other JVM(s). You select the number of backup copies using the `backup-count` property.

```xml
<hazelcast>
  <map name="default">
    <backup-count>1</backup-count>
  </map>
</hazelcast>
```

When this count is 1, a map entry will have its backup on one other node in the cluster. If you set it to 2, then a map entry will have its backup on two other nodes. You can set it to 0 if you do not want your entries to be backed up, e.g. if performance is more important than backing up. The maximum value for this count is 6.

Hazelcast supports both synchronous and asynchronous backups. By default, backup operations are synchronous and configured with `backup-count`. In this case, backup operations block operations until backups are successfully copied to backup nodes (or deleted from backup nodes in case of remove) and acknowledgements are received. Therefore, backups are updated before a `put` operation is completed. Sync backup operations have a blocking cost which may lead to latency issues.

#### Async Backup

Asynchronous backups, on the other hand, do not block operations. They are fire & forget and do not require acknowledgements; the backup operations are performed at some point in time. Async backup is configured using the `async-backup-count` property. An example is shown below.
 

```xml
<hazelcast>
  <map name="default">
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
  </map>
</hazelcast>
```

<br></br>
![image](images/NoteSmall.jpg) ***NOTE:*** *Backups increase memory usage since they are also kept in memory. So for every backup, you  double the original memory consumption.*

![image](images/NoteSmall.jpg) ***NOTE:*** *A map can have both sync and aysnc backups at the same time.*



#### Read Backup Data

By default, Hazelcast has one sync backup copy. If `backup-count` is set to more than 1, then each member will carry both owned entries and backup copies of other members. So for the `map.get(key)` call, it is possible that the calling member has a backup copy of that key. By default, `map.get(key)` will always read the value from the actual owner of the key for consistency.
It is possible to enable backup reads (read local backup entries) by setting the value of the `read-backup-data` property to **true**. Its default value is **false** for strong consistency. Enabling backup reads can improve performance. 

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
