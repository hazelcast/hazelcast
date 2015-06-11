
## RingBuffer Configuration


The following are example RingBuffer configurations.


**Declarative:**

```xml
<ringbuffer name="default">
    <capacity>1000</capacity>
    <time-to-live-seconds>0</time-to-live-seconds>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <in-memory-format>BINARY</in-memory-format>
</ringbuffer>
```

**Programmatic:**

```java
Config config = new Config();
RingbufferConfig rbConfig = config.getRingbufferConfig();
rbConfig.setCapacity( 1000 )
        .setTimeToLiveSeconds( 0 )
        .setBackupCount( 1 )
        .setAsyncBackupCount( 0 )
        .setInMemoryFormat( "BINARY" );
```

RingBuffer configuration has the following elements.

- `capacity`: Total number of items in the RingBuffer. The default value is 10000.
- `time-to-live-seconds`: Duration that the RingBuffer retains the items before deleting them. When it is set to **0**, it will be disabled. The default value is 0.
- `backup-count`: Number of synchronous backups. The default value is 1.
- `async-backup-count`: Number of asynchronous backups. The default value is 0.
- `in-memory-format`: In-memory format of an item when stored in the RingBuffer. Available values are `OBJECT` and `BINARY`. The default value is `BINARY`. 

