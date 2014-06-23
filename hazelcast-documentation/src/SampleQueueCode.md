
### Sample Queue Code

Below sample codes illustrate a producer and consumer connected by a distributed queue.

Let's put one integer at each second on a queue, 100 integers in total.

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

public class ProducerMember {
    public static void main(String[] args) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IQueue<Integer> queue = hz.getQueue("queue");
        for (int k = 1; k < 100; k++) {
            queue.put(k);
            System.out.println("Producing: " + k);
            Thread.sleep(1000);
        }
        queue.put(-1);
        System.out.println("Producer Finished!");
    }
}
``` 

`Producer` puts a **-1** on the queue to show that `put`'s are finished. Now, let's create a `Consumer` class that take a message from this queue, as shown below.


```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

public class ConsumerMember {
    public static void main(String[] args) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IQueue<Integer> queue = hz.getQueue("queue");
        while (true) {
            int item = queue.take();
            System.out.println("Consumed: " + item);
            if (item == -1) {
                queue.put(-1);
                break;
            }
            Thread.sleep(5000);
        }
        System.out.println("Consumer Finished!");
    }
}
```

As seen in the above sample code, `Consumer` waits 5 seconds before it consumes the next message. It stops once it receives **-1**. Also note that, `Consumer` puts **-1** back on the queue before the loop is ended. 

When you start first the `Producer` and then the `Consumer`, items produced on the queue will be consumed from that same queue.

From the above codes, you can see that item production is done at every second, and the consumption is performed at every 5 seconds. So, it can be realized that the consumer keeps growing. To balance the produce/consume operation, let's start another consumer. By this way, consumption is distributed to these two consumers, see the sample outputs below. 

Once the second consumer is started after a while, first consumer output:

```
...
Consumed 13 
Consumed 15
Consumer 17
...
```

Second consumer output:

```
...
Consumed 14 
Consumed 16
Consumer 18
...
```

In the case of a lot of producers and consumers for the queue, using a list of queues may solve the queue bottlenecks. Of course, in this case, you should be aware that ordering of messages being sent to different queues is not guaranteed. But, since in most cases strict ordering is not important, list of queues would be a good solution.

<font color='red'>***Note***:</font> *The items are taken from the queue in the same order they were put. However, if there are more than one consumers, this ordering is not guaranteed.*
