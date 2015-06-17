## Reliable Topic

In Hazelcast 3.5 the reliable Topic has been introduced. The reliable topic makes use of the same ITopic interface
as the regular topic, but the main difference is that it is backed up by the Ringbuffer (also introduced in Hazelcast 
3.5). There are quite a few advantages to this approach:
* events won't get lost since the Ringbuffer is by default configured with 1 synchronous backup
* better isolation part I: each reliable ITopic gets its own Ringbuffer; so if there is topic with a very fast producer, it will not lead to problems at topic that run at a slower pace.
* better isolation part II: since the event system that sits behind a regular ITopic is shared with other data-structures e.g. collection listeners, 
  you can run into isolation problems. This won't happen with the reliable ITopic.

### Sample reliable ITopic Code

```java
import com.hazelcast.core.Topic;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MessageListener;

public class Sample implements MessageListener<MyEvent> {

  public static void main( String[] args ) {
    Sample sample = new Sample();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ITopic topic = hazelcastInstance.getReliableTopic( "default" );
    topic.addMessageListener( sample );
    topic.publish( new MyEvent() );
  }

  public void onMessage( Message<MyEvent> message ) {
    MyEvent myEvent = message.getMessageObject();
    System.out.println( "Message received = " + myEvent.toString() );
  }
}
```

The reliable ITopic can be configured using its Ringbuffer. So if there is a reliable topic with name 'Foo', then this topic can
be configured by adding a ReliableTopicConfig for a ringbuffer with name 'Foo'. By default a Ringbuffer doesn't have any TTL and
it has a limited capacity, so probably you want to change the configuration.

By default the reliable ITopic makes use of a shared threadpool. If you need better isolation, a custom executor can be set on the 
ReliableTopicConfig. 

Because the reads on a Ringbuffer are not destructive, it is easy to apply batching. By default the ITopic uses read-batching and reads
10 items at a time (if available).

### Slow consumers
The reliable ITopic provides control and how to deal with slow consumers. It is unwise to keep events for a slow consumer in memory 
indefinitely since you don't know when it is going to catch up. The size of the Ringbuffer can be controlled using its capacity. But
what should happen when the Ringbuffer runs out of capacity? This can be controlled using the TopicOverflowPolicy:
* DISCARD_OLDEST: just overwrite the oldest item, no matter if a TTL is set. In this case the fast producer is favor above a slow consumer
* DISCARD_NEWEST: discard the newest item.
* BLOCK: wait till items are expired in the ringbuffer.
* FAIL: Immediately throw TopicOverloadException if there is no space in Ringbuffer.