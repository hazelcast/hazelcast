

## Distributed Lock

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.config.Config;
import java.util.concurrent.locks.Lock;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
Lock lock = hz.getLock(myLockedObject);
lock.lock();
try {
    // do something here
} finally {
    lock.unlock();
} 
 
```
`java.util.concurrent.locks.Lock.tryLock()` with timeout is also supported. All operations on the Lock that `Hazelcast.getLock(Object obj)` returns are cluster-wide and Lock behaves just like `java.util.concurrent.lock.ReentrantLock`.

```java
if (lock.tryLock (5000, TimeUnit.MILLISECONDS)) {
    try {  
       // do some stuff here..  
    } 
    finally {  
      lock.unlock();  
    }   
} 
```

Locks are fail-safe. If a member holds a lock and some of the members go down, cluster will keep your locks safe and available. Moreover, when a member leaves the cluster, all the locks acquired by this dead member will be removed so that these locks can be available for live members immediately.
