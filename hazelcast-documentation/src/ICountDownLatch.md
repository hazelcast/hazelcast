

## ICountDownLatch



Hazelcast ICountDownLatch is the distributed implementation of `java.util.concurrent.CountDownLatch`. As you may know, CountDownLatch is considered to be a gate keeper for concurrent activities. It enables the threads to wait for other threads to complete their operations.

Below sample codes describe the mechanism of ICountDownLatch. Assume that there is a leader process and there are follower ones that will wait until the leader completes. Here is the leader:

```java
public class Leader {
  public static void main( String[] args ) throws Exception {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ICountDownLatch latch = hazelcastInstance.getCountDownLatch( "countDownLatch" );
    System.out.println( "Starting" );
    latch.trySetCount( 1 );
    Thread.sleep( 30000 );
    latch.countDown();
    System.out.println( "Leader finished" );
    latch.destroy();
  }
}
```

Since only a single step is needed to be completed as a sample, above code initializes the latch with 1. Then, sleeps for a while to simulate a process and starts the countdown. Finally, it clears up the latch. And now, let's write a follower:


```java
public class Follower {
  public static void main( String[] args ) throws Exception {
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    ICountDownLatch latch = hazelcastInstance.getCountDownLatch( "countDownLatch" );
    System.out.println( "Waiting" );
    boolean success = latch.await( 10, TimeUnit.SECONDS );
    System.out.println( "Complete: " + success );
  }
} 
```

The follower class above first retrieves ICountDownLatch and then calls the `await` method to enable the thread to listen for the latch. The method `await` has a timeout value as a parameter. This is useful when `countDown` method fails. To see ICountDownLatch on action, start the leader first and then start one or more followers. You will see that followers will wait until the leader completes.

In a distributed environment, it is possible that the counting down cluster member may go down. In this case, all listeners are notified immediately and automatically by Hazelcast. Of course, state of the current process just before the failure should be verified and 'how to continue now' should be decided (e.g. restart all process operations, continue with the first failed process operation, throw an exception, etc.).

Although the ICountDownLatch is a very useful synchronization aid, it probably isn’t one you will use on a daily basis. Unlike Java’s implementation, Hazelcast’s ICountDownLatch count can be re-set after a countdown has finished but not during an active count.

***ATTENTION:*** *ICountDownLatch has 1 synchronous backup and no asynchronous backups. Its backup count is not configurable. Also, the count cannot be re-set during an active count, it should be re-set after the countdown is finished.*

