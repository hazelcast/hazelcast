
### Query Thread Configuration

Size of the thread pool dedicated to query operations can be changed using the `pool-size` property. Below is a sample declarative configuration.

```xml
<executor-service name="hz:query">
  <pool-size>100</pool-size>
</executor-service>
```

And, below is the programmatic configuration equivalent to the sample above.

```java
Config cfg = new Config();
cfg.getExecutorConfig("hz:query").setPoolSize(100);
```
