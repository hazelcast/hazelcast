

## Semaphore Configuration

The following are the example configurations.

**Declarative:**

```xml
<semaphore name="semaphore">
   <backup-count>1</backup-count>
   <async-backup-count>0</async-backup-count>
   <initial-permits>3</initial-permits>
</semaphore>
```

**Programmatic:**

```java
Config config = new Config();
SemaphoreConfig semaphoreConfig = config.getSemaphoreConfig();
semaphoreConfig.setName( "semaphore" ).setBackupCount( "1" )
        .setInitialPermits( "3" );
```

It has below elements.

- `initial-permits`: It is the thread count which the concurrent access is limited to. For example, if you set it to "3", concurrent access to the object is limited to 3 threads.
- `backup-count`: Defines the count of synchronous backups. 
- `async-backup-count`: Defines the count of asynchronous backups. 

