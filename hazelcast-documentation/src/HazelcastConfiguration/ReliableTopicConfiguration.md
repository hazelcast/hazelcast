
## Reliable Topic Configuration


The following are example Reliable Topic configurations.


**Declarative:**

```xml
<reliable-topic name="default">
    <statistics-enabled>true</statistics-enabled>
    <message-listeners>
        <message-listener>
        
        </message-listener>
    </message-listeners>
    <read-batch-size>10</read-batch-size>
    <topic-overload-policy>BLOCK</topic-overload-policy>
</reliable-topic>
```

**Programmatic:**

```java
Config config = new Config();
ReliableTopicConfig rtConfig = config.getReliableTopicConfig();
rtConfig.setTopicOverloadPolicy( "BLOCK" )
        .setReadBatchSize( 10 )
        .setStatisticsEnabled( "true" );
```

Reliable Topic configuration has the following elements.

- `statistics-enabled`: Enables or disables the statistics collection for the Reliable Topic. The default value is `true`.
- `message-listener`: Message listener class that listens to the messages when they are added or removed.
- `read-batch-size`: Minimum number of messages that Reliable Topic will try to read in batches. The default value is 10.
- `topic-overload-policy`: Policy to handle an overloaded topic. Available values are `DISCARD_OLDEST`, `DISCARD_NEWEST`, `BLOCK` and `ERROR`. The default value is `BLOCK.

