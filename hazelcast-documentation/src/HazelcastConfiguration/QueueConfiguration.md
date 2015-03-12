
## Queue Configuration


The following are example queue configurations.


**Declarative:**

```xml
<queue name="default">
    <max-size>0</max-size>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <empty-queue-ttl>-1</empty-queue-ttl>
    <item-listeners>
       <item-listener>
          com.hazelcast.examples.ItemListener
       </item-listener>
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

Queue configuration has the following elements.

- `max-size`: Maximum number of items in the Queue.
- `backup-count`: Number of synchronous backups. Queue is a non-partitioned data structure, so all entries of a Set resides in one partition. When this parameter is '1', it means there will be 1 backup of that Set in another node in the cluster. When it is '2', 2 nodes will have the backup.
- `async-backup-count`: Number of asynchronous backups.
- `empty-queue-ttl`: Used to purge unused or empty queues. If you define a value (time in seconds) for this element, then your queue will be destroyed if it stays empty or unused for that time.
- `item-listeners`: Lets you add listeners (listener classes) for the queue items. You can also set the attribute `include-value` to `true` if you want the item event to contain the item values, and you can set `local` to `true` if you want to listen to the items on the local node.
- `queue-store`: Includes the queue store factory class name and the properties  *binary*, *memory limit* and *bulk load*. Please refer to [Queue Persistence](#queue-persistence).
- `statistics-enabled`: If set to `true`, you can retrieve statistics for this Queue using the method `getLocalQueueStats()`.

