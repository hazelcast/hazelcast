

### ICondition

`ICondition` is the distributed implementation of the `notify`, `notifyAll` and `wait` operations on the Java object. You can use it to synchronize
threads across the cluster. More specifically, you use `ICondition` when a thread's work depends on another thread's output. A good example
can be producer/consumer methodology. 

Please see the below code snippets for a sample producer/consumer implementation.

**Producer thread:**

```java
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Lock lock = hazelcastInstance.getLock( "myLockId" );
ICondition condition = lock.newCondition( "myConditionId" );

lock.lock();
try {
  while ( !shouldProduce() ) {
    condition.await(); // frees the lock and waits for signal
                       // when it wakes up it re-acquires the lock
                       // if available or waits for it to become
                       // available
  }
  produce();
  condition.signalAll();
} finally {
  lock.unlock();
}
```

**Consumer thread:**
       
```java       
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Lock lock = hazelcastInstance.getLock( "myLockId" );
ICondition condition = lock.newCondition( "myConditionId" );

lock.lock();
try {
  while ( !canConsume() ) {
    condition.await(); // frees the lock and waits for signal
                       // when it wakes up it re-acquires the lock if 
                       // available or waits for it to become
                       // available
  }
  consume();
  condition.signalAll();
} finally {
  lock.unlock();
}
```

