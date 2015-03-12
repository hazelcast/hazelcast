
## Executor Service Configuration

Following are the example configurations for executor service.

**Declarative:**

```xml
<executor-service name="exec">
   <pool-size>1</pool-size>
   <queue-capacity>10</queue-capacity>
   <statistics-enabled>true</statistics-enabled>
</executor-service>
```

**Programmatic:**

```java
Config config = new Config();
ExecutorConfig executorConfig = config.getExecutorConfig();
executorConfig.setPoolSize( "1" ).setQueueCapacity( "10" )
          .setStatisticsEnabled( true );
```

Executor service configuration has the following elements.

- `pool-size`: The number of executor threads per Member for the Executor.
- `queue-capacity`: Executor's task queue capacity.
- `statistics-enabled`: Some statistics like pending operations count, started operations count, completed operations count, cancelled operations count can be retrieved by setting this parameter's value as `true`. The method for retrieving the statistics is `getLocalExecutorStats()`.

