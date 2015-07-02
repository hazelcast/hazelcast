## Ringbuffer

Hazelcast Ringbuffer is a distributed data structure that stores its data in a ring-like structure. You can think of it as a circular array with a 
given capacity. Each Ringbuffer has a tail and a head. The tail is where the items are added and the head is where the items are overwritten 
or expired. You can reach each element in a Ringbuffer using a sequence ID, which is mapped to the elements between the head 
and tail (inclusive) of the Ringbuffer. 

Reading from Ringbuffer is simple: get its current head with the `headSequence` method and start reading. Use the method `readOne` to return the item at the 
given sequence; `readOne` blocks if no item is available. To read the next item, increment the sequence by one.

```java
Ringbuffer<String> ringbuffer = hz.getRingbuffer("rb");
long sequence = ringbuffer.headSequence();
while(true){
    String item = ringbuffer.readOne(sequence);
    sequence++;
    ... process item
}  
```

By exposing the sequence, you can now move the item from the Ringbuffer as long as the item is still available. If the item is not available
any longer, `StaleSequenceException` is thrown.

Adding an item to Ringbuffer is also easy:

```java
Ringbuffer<String> ringbuffer = hz.getRingbuffer("rb");
ringbuffer.add("someitem")
```

Use the method `add` to returns the sequence of the inserted item; the sequence value will always be unique. You can use this as a 
very cheap way of generating unique IDs if you are already using Ringbuffer.


### IQueue vs. Ringbuffer

Hazelcast Ringbuffer can sometimes be a better alternative than an Hazelcast IQueue. Unlike IQueue, Ringbuffer does not remove the items, it only
reads items using a certain position. There are many advantages to this approach:

* The same item can be read multiple times by the same thread; this is useful for realizing semantics of read-at-least-once or 
read-at-most-once.
* The same item can be read by multiple threads. Normally you could use an IQueue per thread for the same semantic, but this is 
less efficient because of the increased remoting. A take from an IQueue is destructive, so the change needs to be applied for backup 
also, which is why a `queue.take()` is more expensive than a `ringBuffer.read(...)`.
* Reads are extremely cheap since there is no change in the Ringbuffer, therefore no replication is required. 
* Reads and writes can be batched to speed up performance. Batching can dramatically improve the performance of Ringbuffer.
 

### Configuring Ringbuffer Capacity

By default, a Ringbuffer is configured with a `capacity` of 10000 items. This creates an array with a size of 10000. If 
a `time-to-live` is configured, then an array of longs is also created that stores the expiration time for every item. 
In a lot of cases, you may want to change this `capacity` number to something that better fits your needs. 

Below is a declarative configuration example of a Ringbuffer with a `capacity` of 2000 items.

```xml
<ringbuffer name="rb">
    <capacity>2000</capacity>
</ringbuffer>
```

Currently, Hazelcast Ringbuffer is not a partitioned data structure; its data is stored in a single partition and the replicas
 are stored in another partition. Therefore, create a Ringbuffer that can safely fit in a single cluster member. 


### Backing Up Ringbuffer

Hazelcast Ringbuffer has 1 single synchronous backup by default. You can control the Ringbuffer backup just like most of the other Hazelcast 
distributed data structures by setting the synchronous and asynchronous backups: `backup-count` and `async-backup-count`. In the example below, a Ringbuffer is configured with 0
synchronous backups and 1 asynchronous backup:

```xml
<ringbuffer name="rb">
    <backup-count>0</backup-count>
    <async-backup-count>1</async-backup-count>
</ringbuffer>
```

An asynchronous backup will probably give you better performance. However, there is a chance that the item added will be lost 
when the member owning the primary crashes before the backup could complete. You may want to consider batching
methods if you need high performance but do not want to give up on consistency.


### Configuring Ringbuffer Time To Live

You can configure Hazelcast Ringbuffer with a time to live in seconds. Using this setting, you can control how long the items remain in 
the Ringbuffer before they are expired. By default, the time to live is set to 0, meaning that unless the item is overwritten, 
it will remain in the Ringbuffer indefinitely. If you set a time to live and an item is added, then depending on the Overflow Policy, 
either the oldest item is overwritten, or the call is rejected. 

In the example below, a Ringbuffer is configured with a time to live of 180 seconds.

```xml
<ringbuffer name="rb">
    <time-to-live-seconds>180</time-to-live-seconds>
</ringbuffer>
```


### Setting Ringbuffer Overflow Policy

Using the overflow policy, you can determine what to do if the oldest item in the Ringbuffer is not old enough to expire when
 more items than the configured RingBuffer capacity are being added. The below options are currently available.
 
* `OverflowPolicy.OVERWRITE`: The oldest item is overwritten. 
* `OverflowPolicy.FAIL`: The call is aborted. The methods that make use of the OverflowPolicy return `-1` to indicate that adding
the item has failed. 

Overflow policy gives you fine control on what to do if the Ringbuffer is full. You can also use the overflow policy to apply 
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


### Configuring Ringbuffer In-Memory Format

You can configure Hazelcast Ringbuffer with an in-memory format which controls the format of the Ringbuffer's stored items. By default, `BINARY` in-memory format is used, 
meaning that the object is stored in a serialized form. You can select the `OBJECT` in-memory format, which is useful when filtering is 
applied or when the `OBJECT` in-memory format has a smaller memory footprint than `BINARY`. 

In the declarative configuration example below, a Ringbuffer is configured with the `OBJECT` in-memory format:

```xml
<ringbuffer name="rb">
    <in-memory-format>BINARY</in-memory-format>
</ringbuffer>
```


### Adding Batched Items

In the previous examples, the method `ringBuffer.add()` is used to add an item to the Ringbuffer. The problem with this method 
is that it always overwrites and that it does not support batching. Batching can have a huge
impact on the performance. You can use the method `addAllAsync` to support batching. 

Please see the following example code.

```java
List<String> items = Arrays.asList("1","2","3");
ICompletableFuture<Long> f = rb.addAllAsync(items, OverflowPolicy.OVERWRITE);
f.get()
```  
      
In the above case, three strings are added to the Ringbuffer using the policy `OverflowPolicy.OVERWRITE`. Please see the [Overflow Policy section](#overflow-policy) 
for more information.

### Reading Batched Items

In the previous example, the `readOne` method read items from the Ringbuffer. `readOne` is simple but not very efficient for the following reasons:

* `readOne` does not use batching.
* `readOne` cannot filter items at the source; the items need to be retrieved before being filtered.

The method `readManyAsync` can read a batch of items and can filter items at the source. 

Please see the following example code.

```java
ICompletableFuture<ReadResultSet<E>> readManyAsync(
   long startSequence, 
   int minCount,                                              
   int maxCount, 
   IFunction<E, Boolean> filter);
```

The meanings of the `readManyAsync` arguments are given below.

* `startSequence`: Sequence of the first item to read.
* `minCount`: Minimum number of items to read. If you do not want to block, set it to 0. If you want to block for at least one item,
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
       
Please take a careful look at how your sequence is being incremented. You cannot always rely on the number of items being returned
if the items are filtered out.


### Using Async Methods

Hazelcast Ringbuffer provides asynchronous methods for more powerful operations like batched writing or batched reading with filtering. 
To make these methods synchronous, just call the method `get()` on the returned future.

Please see the following example code.

```java
ICompletableFuture f = ringbuffer.addAsync(item, OverflowPolicy.FAIL);
f.get();
```

However, you can also use `ICompletableFuture` to get notified when the operation has completed. The advantage of `ICompletableFuture` is that the thread used for the call is not blocked till the response is returned.

Please see the below code as an example of when you want to 
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


### Ringbuffer Configuration Examples

The following shows the declarative configuration of a Ringbuffer called `rb`. The configuration is modeled after the Ringbuffer defaults.

```xml
<ringbuffer name="rb">
    <capacity>10000</capacity>
    <backup-count>1</backup-count>
    <async-backup-count>0</async-backup-count>
    <time-to-live-seconds>0</time-to-live-seconds>
    <in-memory-format>BINARY</in-memory-format>
</ringbuffer>
```

You can also configure a Ringbuffer programmatically. The following is a programmatic version of the above declarative configuration.

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

