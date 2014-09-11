

## Lock

ILock is the distributed implementation of `java.util.concurrent.locks.Lock`. Meaning if you lock on an ILock, the critical
section that it guards is guaranteed to be executed by only one thread in entire cluster. Even though locks are great for synchronization, they can lead to problems if not used properly. And also please note that Hazelcast Lock does not support fairness.

A few warnings when using locks:

- Always use lock with *try*-*catch* blocks. It will ensure that lock will be released if an exception is thrown from
the code in critical section. Also note that lock method is outside *try*-*catch* block, because we do not want to unlock
if lock operation itself fails.

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
lock can wait forever. To avoid this, `tryLock` with a timeout value can be used. One can
set a high value (normally should not take that long) for `tryLock`. Return value of `tryLock` can be checked as follows:

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

- Another method to avoid ending up with indefinitely waiting threads is using lock with lease time. This will cause
lock to be released in the given time. Lock can be unlocked before time expires safely. Note that the unlock operation can
throw `IllegalMonitorStateException` if lock is released because of lease time expiration. If it is the case, it means
that critical section guarantee is broken.

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

- Locks are fail-safe. If a member holds a lock and some other members go down, cluster will keep your locks safe and available.
Moreover, when a member leaves the cluster, all the locks acquired by this dead member will be removed so that these
locks can be available for live members immediately.


- Locks are re-entrant, meaning same thread can lock multiple times on the same lock. Note that for other threads to be
 able to require this lock, owner of the lock should call unlock as many times as it called lock.

- In split-brain scenario, cluster behaves as if there are two different clusters. Since two separate clusters are not aware of each other,
two nodes from different clusters can acquire the same lock.
For more information on places where split brain can be handled, please see [Split Brain](#how-is-split-brain-syndrome-handled).

- Locks are not automatically removed. If a lock is not used anymore, Hazelcast will not automatically garbage collect the lock and
this can lead to an OutOfMemoryError. So if you create locks on the fly, make sure they are destroyed.

- Hazelcast IMap also provides a locking support on the entry level using the method `IMap.lock(key)`. Although the same infrastructure 
is being used, `IMap.lock(key)` is not an ILock and it is not possible to expose it directly.

