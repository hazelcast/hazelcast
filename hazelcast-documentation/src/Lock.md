

## Lock

ILock is the distributed implementation of `java.util.concurrent.locks.Lock`. Meaning if you lock using an ILock, the critical
section that it guards is guaranteed to be executed by only one thread in the entire cluster. Even though locks are great for synchronization, they can lead to problems if not used properly. Also note that Hazelcast Lock does not support fairness.

A few warnings when using locks:

- Always use locks with *try*-*catch* blocks. It will ensure that locks will be released if an exception is thrown from
the code in a critical section. Also note that the lock method is outside the *try*-*catch* block, because we do not want to unlock
if the lock operation itself fails.

```java
import com.hazelcast.core.Hazelcast;
import java.util.concurrent.locks.Lock;

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Lock lock = hazelcastInstance.getLock( "myLock" );
lock.lock();
try {
  // do something here
} finally {
  lock.unlock();
}
```

- If a lock is not released in the cluster, another thread that is trying to get the
lock can wait forever. To avoid this, use `tryLock` with a timeout value. You can
set a high value (normally it should not take that long) for `tryLock`. You can check the return value of `tryLock` as follows:

```java
if ( lock.tryLock ( 10, TimeUnit.SECONDS ) ) {
  try {  
    // do some stuff here..  
  } finally {  
    lock.unlock();  
  }   
} else {
  // warning
}
```

- You can also avoid indefinitely waiting threads by using lock with lease time: the lock will be released in the given lease time. Lock can be safely unlocked before the lease time expires. Note that the unlock operation can
throw an `IllegalMonitorStateException` if lock is released because the lease time expires. If that is the case, critical section guarantee is broken.

	Please see the below example.

```java
lock.lock( 5, TimeUnit.SECONDS )
try {
  // do some stuff here..
} finally {
  try {
    lock.unlock();
  } catch ( IllegalMonitorStateException ex ){
    // WARNING Critical section guarantee can be broken
  }
}
```

- Locks are fail-safe. If a member holds a lock and some other members go down, the cluster will keep your locks safe and available.
Moreover, when a member leaves the cluster, all the locks acquired by that dead member will be removed so that those
locks are immediately available for live members.


- Locks are re-entrant: the same thread can lock multiple times on the same lock. Note that for other threads to be
 able to require this lock, the owner of the lock must call `unlock` as many times as the owner called `lock`.

- In the split-brain scenario, the cluster behaves as if it were two different clusters. Since two separate clusters are not aware of each other,
two nodes from different clusters can acquire the same lock.
For more information on places where split brain syndrome can be handled, please see [split brain syndrome](#network-partitioning-split-brain-syndrome).

- Locks are not automatically removed. If a lock is not used anymore, Hazelcast will not automatically garbage collect the lock. 
This can lead to an `OutOfMemoryError`. If you create locks on the fly, make sure they are destroyed.

- Hazelcast IMap also provides locking support on the entry level with the method `IMap.lock(key)`. Although the same infrastructure 
is used, `IMap.lock(key)` is not an ILock and it is not possible to expose it directly.

