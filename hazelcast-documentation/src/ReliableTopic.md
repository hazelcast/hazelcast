## Reliable Topic

Reliable Topic data structure has been introduced with the release of Hazelcast 3.5. The Reliable Topic makes use of the same `ITopic` interface
as a regular topic. The main difference is that it is backed up by the RingBuffer (also introduced with Hazelcast 
3.5) data structure. The following are the advantages of this approach:

* Events are not lost since the RingBuffer is configured with 1 synchronous backup by default.
* Each Reliable `ITopic` gets its own RingBuffer; if there is a topic with a very fast producer, it will not lead to problems at the topic that runs at a slower pace.
* Since the event system behind a regular `ITopic` is shared with other data structures (e.g. collection listeners), 
  you can run into isolation problems. This does not happen with the Reliable `ITopic`.

### Sample Reliable ITopic Code

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

The Reliable `ITopic` can be configured using its RingBuffer. If there is a Reliable Topic with name `Foo`, then this topic can
be configured by adding a `ReliableTopicConfig` for a RingBuffer with the name `Foo`. By default, a RingBuffer does not have any TTL (time to live) and
it has a limited capacity; you may want to change the configuration.

By default, the Reliable `ITopic` uses a shared thread pool. If you need a better isolation, you can configure a custom executor on the 
`ReliableTopicConfig`. 

Because the reads on a RingBuffer are not destructive, it is easy to apply batching. `ITopic` uses read batching and reads
10 items at a time (if available) by default.

### Slow Consumers

The Reliable `ITopic` provides control and a way to deal with slow consumers. It is unwise to keep events for a slow consumer in memory 
indefinitely since you do not know when it is going to catch up. The size of the RingBuffer can be controlled using its capacity. For the cases when a RingBuffer runs out of its capacity, you can specify the following policies for the `TopicOverloadPolicy` configuration:

* `DISCARD_OLDEST`: Overwrite the oldest item, no matter if a TTL is set. In this case the fast producer supersedes a slow consumer
* `DISCARD_NEWEST`: Discard the newest item.
* `BLOCK`: Wait until the items are expired in the RingBuffer.
* `FAIL`: Immediately throw `TopicOverloadException` if there is no space in the RingBuffer.