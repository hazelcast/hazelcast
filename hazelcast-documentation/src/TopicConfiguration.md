

### Topic Configuration

**Declarative Configuration:**

```xml
<hazelcast>
  ...
  <topic name="yourTopicName">
    <global-ordering-enabled>true</global-ordering-enabled>
    <statistics-enabled>true</statistics-enabled>
    <message-listeners>
      <message-listener>MessageListenerImpl</message-listener>
    </message-listeners>
  </topic>
  ...
</hazelcast>
```

**Programmatic Configuration:**

```java
TopicConfig topicConfig = new TopicConfig();
topicConfig.setGlobalOrderingEnabled( true );
topicConfig.setStatisticsEnabled( true );
topicConfig.setName( "yourTopicName" );
MessageListener<String> implementation = new MessageListener<String>() {
  @Override
  public void onMessage( Message<String> message ) {
    // process the message
  }
};
topicConfig.addMessageListenerConfig( new ListenerConfig( implementation ) );
HazelcastInstance instance = Hazelcast.newHazelcastInstance()
```

Default values are:

- `global-ordering` is **false**, meaning that by default, there is no guarantee of global order.

- `statistics` is **true**, meaning that by default, statistics are calculated.

Topic related but not topic specific configuration parameters:

   - `hazelcast.event.queue.capacity`: default value is 1,000,000
   - `hazelcast.event.queue.timeout.millis`: default value is 250
   - `hazelcast.event.thread.count`: default value is 5
   
<br></br>
***RELATED INFORMATION*** 

*For description of these parameters, please see [Global Event Configuration](#global-event-configuration)*


