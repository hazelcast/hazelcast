

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

Default values are

- Global ordering is **false**, meaning there is no global order guarantee by default.

- Statistics are **true**, meaning statistics are calculated by default.

Topic related but not topic specific configuration parameters

   - `hazelcast.event.queue.capacity`: default value is 1,000,000
   - `hazelcast.event.queue.timeout.millis`: default value is 250
   - `hazelcast.event.thread.count`: default value is 5
   
<br></br>
***RELATED INFORMATION*** 

*For description of these parameters, please see [Global Event Configuration](#global-event-configuration)*


