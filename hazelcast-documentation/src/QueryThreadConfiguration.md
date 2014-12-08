
### Query Thread Configuration

You can change the size of the thread pool dedicated to query operations using the `pool-size` property. Below is an example of that declarative configuration.

```xml
<executor-service name="hz:query">
  <pool-size>100</pool-size>
</executor-service>
```

Below is an example of the equivalent programmatic configuration.

```java
Config cfg = new Config();
cfg.getExecutorConfig("hz:query").setPoolSize(100);
```
