## Ringbuffer

Hazelcast Ringbuffer is a distributed data structure where the data is stored in a ring-like structure. You can think of it as a circular array with a 
certain capacity. Each Ringbuffer has a tail and a head. The tail is where the items are added and the head is where the items are overwritten 
or expired. You can reach each element in a Ringbuffer using a sequence ID, which is mapped to the elements between the head 
and tail (inclusive) of the Ringbuffer. 

Reading from Ringbuffer is very simple. Just get the current head and start reading. The method `readOne` returns the item at the 
given sequence or blocks if no item is available. To read the next item, the sequence is incremented by one.

```java
Ringbuffer<String> ringbuffer = hz.getRingbuffer("rb");
long sequence = ringbuffer.headSequence();
while(true){
    String item = ringbuffer.readOne(sequence);
    sequence++;
    ... process item
}  
```

By exposing the sequence, you can now move the item from Ringbuffer as long as the item is still available. If it is not available
any longer, `StaleSequenceException` is thrown.

Adding an item to Ringbuffer is also very easy:

```java
Ringbuffer<String> ringbuffer = hz.getRingbuffer("rb");
ringbuffer.add("someitem")
```

The method `add` returns the sequence of the inserted item and this value will always be unique. This can sometimes be used as a 
very cheap way of generating unique IDs if you are already using Ringbuffer.


### IQueue vs. Ringbuffer

Hazelcast Ringbuffer can sometimes be a better alternative than an Hazelcast IQueue. Unlike IQueue, Ringbuffer does not remove the items, it only
reads items using a certain position. There are many advantages using this approach:

* The same item can be read multiple times by the same thread; this is useful for realizing semantics of read-at-least-once or 
read-at-most-once.
* The same item can be read by multiple threads. Normally you could use an IQueue per thread for the same semantic, but this is 
less efficient because of the increased remoting. A take from an IQueue is destructive, so the change needs to be applied for backup 
also, which is why a `queue.take()` is more expensive than a `ringBuffer.read(...)`.
* Reads are extremely cheap since there is no change in the Ringbuffer, therefore no replication is required. 
* Reads and writes can be batched to speed up performance. Batching can dramatically improve the performance of Ringbuffer.
 

### Capacity

By default, a Ringbuffer is configured with a capacity of 10000 items. Internally, an array is created with exactly that size. If 
a time-to-live is configured, then an array of longs is also created that stores the expiration time for every item. 
In a lot of cases, you may want to change this number to something that fits your needs better. 

Below is a declarative configuration example of a Ringbuffer with a capacity of 2000 items.

```xml
<ringbuffer name="rb">
    <capacity>2000</capacity>
</ringbuffer>
```

Hazelcast Ringbuffer is not a partitioned data structure in its current state; its data is stored in a single partition and the replicas
 are stored in another partition. Therefore, create a Ringbuffer that can safely fit in a single cluster member. 


### Synchronous and Asynchronous Backups

Hazelcast Ringbuffer has a single synchronous backup by default. This can be controlled just like most of the other Hazelcast 
distributed data structures by setting the sync and async backups. In the example below, a Ringbuffer is configured with 0
sync backups and 1 async backup:

```xml
<ringbuffer name="rb">
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
</ringbuffer>
```

An async backup will probably give you better performance. However, there is a chance that the item added is lost 
when the member owning the primary crashes before the replication could complete. You may want to consider batching
methods if you need high performance but do not want to give up on consistency.


### Time to live

Hazelcast Ringbuffer can be configured with a time to live seconds. Using this setting, you can control how long the items remain in 
the Ringbuffer before they are expired. By default, the time to live is set to 0, meaning that unless the item is overwritten, 
it will remain in the Ringbuffer indefinitely. If a time to live is set and an item is added, then depending on the Overflow Policy, 
either the oldest item is overwritten, or the call is rejected. 

In the example below, a Ringbuffer is configured with a time to live of 180 seconds.

```xml
<ringbuffer name="rb">
    <time-to-live-seconds>180</time-to-live-seconds>
</ringbuffer>
```


### Overflow Policy

Using the overflow policy, you can determine what to do if the oldest item in the Ringbuffer is not old enough to expire when
 more items than the configured RingBuffer capacity are being added. There are currently below options available:
 
* `OverflowPolicy.OVERWRITE`: The oldest item is overwritten. 
* `OverflowPolicy.FAIL`: The call is aborted. The methods that make use of the OverflowPolicy return `-1` to indicate that adding
the item has failed. 

Overflow policy gives fine control on what to do if the Ringbuffer is full. The policy can also be used to apply 
a back pressure mechanism. The following example code shows the usage of an exponential backoff.

```java
long sleepMs = 100;
for (; ; ) {
    long result = ringbuffer.addAsync(item, OverflowPolicy.FAIL).get();
    if (result != -1) {
        break;
    }
    
    TimeUnit.MILLISECONDS.sleep(sleepMs);
    sleepMs = min(5000, sleepMs * 2);
}
```


### In-Memory Format

Hazelcast Ringbuffer can also be configured with an in-memory format which controls the format of stored items. By default, `BINARY` is used, 
meaning that the object is stored in a serialized form. You can select the `OBJECT` in-memory format, which is useful when filtering is 
applied or when the `OBJECT` in-memory format has a smaller memory footprint than `BINARY`. 

In the declarative configuration example below, a Ringbuffer is configured with `OBJECT` in-memory format:

```xml
<ringbuffer name="rb">
    <in-memory-format>BINARY</in-memory-format>
</ringbuffer>
```


### Adding Batched Items

In the previous examples, the method `ringBuffer.add()` is used to add an item to the Ringbuffer. The problem with this method 
is that it always overwrites and that it does not support batching. Batching can have a huge
impact on the performance. That is why the method `addAllAsync` is available. 

Please see the following example code.

```java
List<String> items = Arrays.asList("1","2","3");
ICompletableFuture<Long> f = rb.addAllAsync(items, OverflowPolicy.OVERWRITE);
f.get()
```  
      
In the above case, three strings are added to the Ringbuffer using the policy `OverflowPolicy.OVERWRITE`. Please see the [Overflow Policy section](#overflow-policy) 
for more information.

### Reading Batched Items

In the previous example the `readOne` was being used. It is simple but not very efficient for the following reasons:

* It does not make use of batching.
* It cannot filter items at the source; they need to be retrieved before being filtered.

That is why the method `readManyAsync` is available.

Please see the following example code.

```java
ICompletableFuture<ReadResultSet<E>> readManyAsync(
   long startSequence, 
   int minCount,                                              
   int maxCount, 
   IFunction<E, Boolean> filter);
```

This call can read a batch of items and can filter items at the source. The meaning of the arguments are given below.

* `startSequence`: Sequence of the first item to read.
* `minCount`: Minimum number of items to read. If you do not want to block, set it to 0. If you do want to block for at least one item,
set it to 1.
* `maxCount`: Maximum number of the items to retrieve. Its value cannot exceed 1000.
* `filter`: A function that accepts an item and checks if it should be returned. If no filtering should be applied, set it to null.

A full example is given below.

```java
long sequence = rb.headSequence();
for(;;) {
    ICompletableFuture<ReadResultSet<String>> f = rb.readManyAsync(sequence, 1, 10, null);
    ReadResultSet<String> rs = f.get();
    for (String s : rs) {
        System.out.println(s);
    }
    sequence+=rs.readCount();
}
``` 
       
Please take a careful look at how the sequence is being incremented. You cannot always rely on the number of items being returned
if the items are filtered out.


### Async Methods

Hazelcast Ringbuffer provides asynchronous methods for more powerful operations like batched reading with filtering or batched writing. 
To make these methods synchronous, just call the method `get()` on the returned future.

Please see the following example code.

```java
ICompletableFuture f = ringbuffer.addAsync(item, OverflowPolicy.FAIL);
f.get();
```

However, the `ICompletableFuture` can also be used to get notified when the operation has completed. Please see the example code when you want to 
get notified when a batch of reads has completed.

```java
ICompletableFuture<ReadResultSet<String>> f = rb.readManyAsync(sequence, min, max, someFilter);
f.andThen(new ExecutionCallback<ReadResultSet<String>>() {
   @Override
   public void onResponse(ReadResultSet<String> response) {
        for (String s : response) {
            System.out.println("Received:" + s);
        }
   }

   @Override
   public void onFailure(Throwable t) {
        t.printStackTrace();
   }
});
```

The advantage of this approach: The thread that is used for the call is not blocked till the response is returned.

### Full Configuration examples

The following shows the declarative configuration of a Ringbuffer called `rb`. The configuration is modeled after Ringbuffer defaults.

```xml
<ringbuffer name="rb">
    <capacity>10000</capacity>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <time-to-live-seconds>0</time-to-live-seconds>
    <in-memory-format>BINARY</in-memory-format>
</ringbuffer>
```

You can also configure a Ringbuffer programmatically. The following is programmatic version of the above declarative configuration.

```java
RingbufferConfig rbConfig = new RingbufferConfig("rb")
    .setCapacity(10000)
    .setBackupCount(1)
    .setAsyncBackupCount(0)
    .setTimeToLiveSeconds(0)
    .setInMemoryFormat(InMemoryFormat.BINARY);
Config config = new Config();
config.addRingbufferConfig(rbConfig);
```        

***RELATED INFORMATION***

*Please refer to the [Ringbuffer Configuration section](#ringbuffer-configuration) for more information on configuring the Ringbuffer.*

