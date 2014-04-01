
## Topic

Hazelcast provides distribution mechanism for publishing messages that are delivered to multiple subscribers which is also known as publish/subscribe (pub/sub) messaging model. Publish and subscriptions are cluster wide. When a member subscribes for a topic, it is actually registering for messages published by any member in the cluster, including the new members joined after you added the listener. 

Messages are ordered, i.e. listeners (subscribers) will process the messages in the order they are actually published. If cluster member M publishes messages *m1*, *m2*, *m3*,...,*mn* to a topic **T**, then Hazelcast makes sure that all of the subscribers of topic **T** will receive and process *m1*, *m2*, *m3*,...,*mn* in the given order. Therefore, there is only single thread invoking `onMessage`. 

There is also `globalOrderEnabled` option in topic configuration, which is disabled by default. When enabled, it guarantees that all nodes listening the same topic will get messages in the same order. The user should not keep the thread busy and preferably dispatch it via an Executor.

```java
import com.hazelcast.core.Topic;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MessageListener;
import com.hazelcast.config.Config;

public class Sample implements MessageListener<MyEvent> {

    public static void main(String[] args) {
        Sample sample = new Sample();
        Config cfg = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        ITopic topic = hz.getTopic ("default");
        topic.addMessageListener(sample);
        topic.publish (new MyEvent());
    }

   public void onMessage(Message<MyEvent> message) {
        MyEvent myEvent = message.getMessageObject();
        System.out.println("Message received = " + myEvent.toString());
        if (myEvent.isHeavyweight()) {
            messageExecutor.execute(new Runnable() {
                public void run() {
                    doHeavyweightStuff(myEvent);
                }
            });
        }
    }

    // ...

    private static final Executor messageExecutor = Executors.newSingleThreadExecutor();
}
```
