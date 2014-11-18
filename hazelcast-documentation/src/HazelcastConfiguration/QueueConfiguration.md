
### Queue Configuration

**Declarative:**

```xml
<queue name="default">
    <max-size>0</max-size>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <empty-queue-ttl>-1</empty-queue-ttl>
    <item-listeners>
       <item-listener>???</item-listener>
    <item-listeners>
</queue>
<queue-store>
    <class-name>com.hazelcast.QueueStoreImpl</class-name>
    <properties>
       <property name="binary">false</property>
       <property name="memory-limit">10000</property>
       <property name="bulk-load">500</property>
    </properties>
</queue-store>   
```

**Programmatic:**

```java
Config config = new Config();
QueueConfig queueConfig = config.getQueueConfig();
queueConfig.setName( "MyQueue" ).setBackupCount( "1" )
        .setMaxSize( "0" ).setStatisticsEnabled( "true" );
queueConfig.getQueueStoreConfig()
        .setEnabled ( "true" )
        .setClassName( "com.hazelcast.QueueStoreImpl" )
        .setProperty( "binary", "false" );
```

It has below attributes and parameters.

- `max-size`: Value of maximum size of items in the Queue.
- `backup-count`: Count of synchronous backups. Remember that, Queue is a non-partitioned data structure, i.e. all entries of a Set resides in one partition. When this parameter is '1', it means there will be a backup of that Set in another node in the cluster. When it is '2', 2 nodes will have the backup.
- `async-backup-count`: Count of asynchronous backups.
- `empty-queue-ttl`: Value of time to live to empty the Queue.
- `item-listeners`: ???
- `queue-store`: Includes the queue store factory class name and the properties  *binary*, *memory limit* and *bulk load*. Please refer to [Queue Persistence](#queue-persistence).
- `statistics-enabled`: If set as `true`, you can retrieve statistics for this Queue using the method `getLocalQueueStats()`.

