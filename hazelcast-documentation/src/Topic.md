## Topic

Hazelcast provides a distribution mechanism for publishing messages that are delivered to multiple subscribers. This is also known as a publish/subscribe (pub/sub) messaging model. Publishing and subscribing operations are cluster wide. When a member subscribes to a topic, it is actually registering for messages published by any member in the cluster, including the new members that joined after you add the listener.

![image](images/NoteSmall.jpg) ***NOTE:*** *Publish operation is async. It does not wait for operations to run in remote nodes, it works as fire and forget.*

### Sample Topic Code

```java
import com.hazelcast.core.Topic;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MessageListener;

public class Sample implements MessageListener<MyEvent> {

  public static void main( String[] args ) {
    Sample sample = new Sample();
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ITopic topic = hazelcastInstance.getTopic( "default" );
    topic.addMessageListener( sample );
    topic.publish( new MyEvent() );
  }

  public void onMessage( Message<MyEvent> message ) {
    MyEvent myEvent = message.getMessageObject();
    System.out.println( "Message received = " + myEvent.toString() );
    if ( myEvent.isHeavyweight() ) {
      messageExecutor.execute( new Runnable() {
          public void run() {
            doHeavyweightStuff( myEvent );
          }
      } );
    }
  }

  // ...

  private final Executor messageExecutor = Executors.newSingleThreadExecutor();
}
```


