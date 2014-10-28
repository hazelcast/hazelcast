
### Sample Queue Code

The following sample code illustrates a producer and consumer connected by a distributed queue.

Let's put one integer on the queue every second, 100 integers total.

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

public class ProducerMember {
  public static void main( String[] args ) throws Exception {
    HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    IQueue<Integer> queue = hz.getQueue( "queue" );
    for ( int k = 1; k < 100; k++ ) {
      queue.put( k );
      System.out.println( "Producing: " + k );
      Thread.sleep(1000);
    }
    queue.put( -1 );
    System.out.println( "Producer Finished!" );
  }
}
``` 

`Producer` puts a **-1** on the queue to show that the `put`'s are finished. Now, let's create a `Consumer` class that take a message from this queue, as shown below.


```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

public class ConsumerMember {
  public static void main( String[] args ) throws Exception {
    HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    IQueue<Integer> queue = hz.getQueue( "queue" );
    while ( true ) {
      int item = queue.take();
      System.out.println( "Consumed: " + item );
      if ( item == -1 ) {
        queue.put( -1 );
        break;
      }
      Thread.sleep( 5000 );
    }
    System.out.println( "Consumer Finished!" );
  }
}
```

As seen in the above sample code, `Consumer` waits 5 seconds before it consumes the next message. It stops once it receives **-1**. Also note that `Consumer` puts **-1** back on the queue before the loop is ended. 

When you first start `Producer` and then start `Consumer`, items produced on the queue will be consumed from the same queue.

From the above sample code, you can see that an item is produced every second, and consumed every 5 seconds. Therefore, the consumer keeps growing. To balance the produce/consume operation, let's start another consumer. By this way, consumption is distributed to these two consumers, as seen in the sample outputs below. 

The second consumer is started. After a while, here is the first consumer output:

```plain
...
Consumed 13 
Consumed 15
Consumer 17
...
```

Here is the second consumer output:

```plain
...
Consumed 14 
Consumed 16
Consumer 18
...
```

In the case of a lot of producers and consumers for the queue, using a list of queues may solve the queue bottlenecks. In this case, be aware that the order of the messages being sent to different queues is not guaranteed. Since in most cases strict ordering is not important, a list of queues is a good solution.

![image](images/NoteSmall.jpg) ***NOTE:*** *The items are taken from the queue in the same order they were put on the queue. However, if there is more than one consumer, this order is not guaranteed.*
