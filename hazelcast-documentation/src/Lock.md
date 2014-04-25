

## Lock

ILock is distributed version of the java.util.concurrent.locks.Lock. Meaning if you lock on an ILock the critical
section that it guards is guaranteed to be executed by only one thread in entire cluster.

Even though Locks are great for synchronization, they can cause problems itself if not used properly.
A few warnings when using locks :

- Always use lock with try,catch blocks. It will ensure that lock will be released if an exception is thrown from
the code in critical section. Also note that lock method is outside try catch block, because we don't want to unlock
if lock operation itself fails.

```java

    import com.hazelcast.core.Hazelcast;
    import com.hazelcast.config.Config;
    import java.util.concurrent.locks.Lock;

    Config cfg = new Config();
    HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
    Lock lock = hz.getLock("myLock");
    lock.lock();
    try {
        // do something here
    } finally {
        lock.unlock();
    }

```
- If a lock is not released in the cluster , another thread that is trying to get the
lock can wait indefinitely. To avoid having a thread blocked forever, tryLock with timeout value can be used. One can
set a high value (normally should not take that long) for tryLock. Return value of tryLock can be checked as follows :

```java
if (lock.tryLock (10, TimeUnit.SECONDS)) {
    try {  
       // do some stuff here..  
    } 
    finally {  
      lock.unlock();  
    }   
}else{
    // warning
}
```
- Another method to avoid ending up with indefinitely wait threads is using lock with lease time. This will cause
lock to released in given time. Lock can be unlocked before time expires safely. Note that the unlock operation can
throw IllegalMonitorStateException if lock is released because of expiry of lease time. If it is the case, it means
that critical section guarantee is broken.
See the following example :

```java

    lock.lock (5, TimeUnit.SECONDS))
    try {
       // do some stuff here..
    } finally {
      try{
         lock.unlock();
      }catch(IllegalMonitorStateException ex ){
         // WARNING Critical section guarantee can be broken
      }

    }
    }else{
        // warning
    }

```

- Locks are fail-safe. If a member holds a lock and some of the other members go down, cluster will keep your locks safe and available.
Moreover, when a member leaves the cluster, all the locks acquired by this dead member will be removed so that these
locks can be available for live members immediately.


- Locks are re-entrant, meaning same thread can lock multiple times on same lock. Note that for other threads to be
 able to require this lock, owner of the lock should call unlock as many times as it called lock.

### ICondition

ICondition is the distributed implementation of notify , notifyAll , wait operations on java object . It can be used to synchronize
threads  across the cluster. More specifically, it is used when a threads work is depend on another threads output. A good example
can be producer/consumer methodology. See following code snippets for a sample producer/consumer implementation.

- Producer thread

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        Lock lock = hz.getLock("myLockId");
        ICondition condition = lock.newCondition("myConditionId");

        lock.lock();

        try {
            while (!shouldProduce()) {
                condition.await(); //frees the lock and waits for signal
                                   //when it wakes up it re-acquires the lock if available or waits for it to become
                                   // available
            }
            produce()
            condition.signalAll();
        } finally {
            lock.unlock();
        }

- Consumer thread

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        Lock lock = hz.getLock("myLockId");
        ICondition condition = lock.newCondition("myConditionId");

        lock.lock();

        try {
            while (!canConsume()) {
                condition.await(); //frees the lock and waits for signal
                                  //when it wakes up it re-acquires the lock if available or waits for it to become
                                  // available
            }
            consume()
            condition.signalAll();
        } finally {
            lock.unlock();
        }


<br> </br>
