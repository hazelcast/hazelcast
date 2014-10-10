

## ISemaphore

Hazelcast ISemaphore is the distributed implementation of `java.util.concurrent.Semaphore`. As you may know, semaphores offer **permit**s to control the thread counts in the case of performing concurrent activities. To execute a concurrent activity, a thread grants a permit or waits until a permit becomes available. When the execution is completed, permit is released.

***ATTENTION:*** *Semaphore with a single permit may be considered as a lock. But, unlike the locks, when semaphores are used any thread can release the permit and also semaphores can have multiple permits.*

***ATTENTION:*** *Hazelcast Semaphore does not support fairness.*

When a permit is acquired on ISemaphore:

-	if there are permits, number of permits in the semaphore is decreased by one and calling thread performs its activity. If there is contention, the longest waiting thread will acquire the permit before all other threads.
-	if no permits are available, calling thread blocks until a permit comes available. And when a timeout happens during this block, thread is interrupted. In the case where the semaphore
is destroyed, an `InstanceDestroyedException` is thrown.

Below sample code uses an IAtomicLong resource for 1000 times, increments the resource when a thread starts to use it and decrements it when the thread completes.

```java
public class SemaphoreMember {
  public static void main( String[] args ) throws Exception{
    HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(); 
    ISemaphore semaphore = hazelcastInstance.getSemaphore( "semaphore" ); 
    IAtomicLong resource = hazelcastInstance.getAtomicLong( "resource" ); 
    for ( int k = 0 ; k < 1000 ; k++ ) {
      System.out.println( "At iteration: " + k + ", Active Threads: " + resource.get() );
      semaphore.acquire();
      try {
        resource.incrementAndGet();
        Thread.sleep( 1000 );
        resource.decrementAndGet();
      } finally { 
        semaphore.release();
      }
    }
    System.out.println("Finished");
  }
}
```

Let's limit the concurrent access to this resource by allowing at most 3 threads. This can be configured declaratively by setting the `initial-permits` property, as shown below.

```xml
<semaphore name="semaphore"> 
  <initial-permits>3</initial-permits>
</semaphore>
```

![image](images/NoteSmall.jpg) ***NOTE:*** *If there is a shortage of permits while the semaphore is being created, value of this property can be set to a negative number.*

If you execute the above `SemaphoreMember` class 5 times, output will be similar to the following:

`At iteration: 0, Active Threads: 1`

`At iteration: 1, Active Threads: 2`

`At iteration: 2, Active Threads: 3`

`At iteration: 3, Active Threads: 3`

`At iteration: 4, Active Threads: 3`

As can be seen, maximum count of concurrent threads is equal or smaller than 3. If you remove the semaphore acquire/release statements in `SemaphoreMember`, you will see that there is no limitation on the number of concurrent usages.

Hazelcast also provides backup support for ISemaphore. When a member goes down, another member can take over the semaphore with the permit information (permits are automatically released when a member goes down). To enable this, synchronous or asynchronous backup should be configured with the properties `backup-count` and `async-backup-count`(by default, synchronous backup is already enabled).

A sample configuration is shown below.

```xml
<semaphore name="semaphore">
  <initial-permits>3</initial-permits>
  <backup-count>1</backup-count>
</semaphore>
```

***ATTENTION:*** *If high performance is more important (than not losing the permit information), you can disable the backups by setting `backup-count` to 0.*

