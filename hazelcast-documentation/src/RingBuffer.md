## RingBuffer


The RingBuffer is a data structure where the data is stored in a ring-like structure. You can think of it as a circular array with a certain capacity. In this circular array, the oldest item gets overwritten in case a new item is written when the maximum capacity is reached. For now, the RingBuffer is not a partitioned data structure; its data is stored in a single partition and the replicas are stored in another partition.

Each element in a RingBuffer can be accessed using a sequence ID. This ID is between the head and tail (inclusive) of the RingBuffer. Head is the side where items are discarded and tail is the side where items are added to.

The RingBuffer can sometimes be a better alternative than an IQueue.  Unlike IQueue, the RingBuffer does not remove the items, it only reads the items using a certain position. There are many advantages using this approach:
* the same item can be read multiple times by the same thread; useful for realizing read at least once or read at most once semantics
* the same item can be read by multiple threads. Normally you could use a IQueue per thread for the same semantic, but this is way less efficient 
* reads are extremely cheap since there is no change in the ringbuffer, there is no change and therefor no replication required. 
* reads can be batched to speed up performance. Using read (and write) batching can dramatically improve performance of the ringbuffer.


The following are the methods included in the RingBuffer interface.

```java
public interface Ringbuffer<E> extends DistributedObject {
  long capacity();
  long size();
  long tailSequence();
  long headSequence();
  long remainingCapacity();
  long add(E item);
  ICompletableFuture<Long> addAsync(E item, OverflowPolicy overflowPolicy);
  E readOne(long sequence) throws InterruptedException;
  ICompletableFuture<Long> addAllAsync(Collection<? extends E> collection, 
                        OverflowPolicy overflowPolicy);
  ICompletableFuture<ReadResultSet<E>> readManyAsync(long startSequence, 
                         int minCount, int maxCount, 
                         IFunction<E, Boolean> filter);
```


The Ringbuffer can be configured with a time to live in seconds. Using this setting you can control how long the items remain in the Ringbuffer before getting deleted. By default the time to live is set to 0, meaning that unless the item is overwritten, it will remain in the ringbuffer indefinitely. If a time to live is set and an item is added, then depending on the OverwritePolicy, either the oldest item is overwritten, or the call is rejected. 

The Ringbuffer can also be configured with an InMemoryFormat which control the format stored items. By default BINARY is used; meaning that the object is stored in serialized form. But also the OBJECT InMemoryFormat can be selected. This is useful when filtering is applied or when the OBJECT in memory format can lead to a smaller memory footprint than a BINARY in memory format. 

The Ringbuffer supports filtered reads. For example when one thread only wants to see certain messages, one can filter the items after they are received from the Ringbuffer. The problem is that this approach can be very inefficient since a lot of useless data needs to be send over the line. When a filter is used, then the filtering happens at the source, which makes it a lot more efficient.

The Ringbuffer provides asynchronous methods for the more powerful methods like batched reading with filtering or batch writing. To make these methods synchronous, just call get() on the returned future. If you don't want to block, a 

For more details about Ringbuffer configuration check the RingbufferConfig class.

***RELATED INFORMATION***

*Please refer to the [RingBuffer Configuration section](#ringbuffer-configuration) for more information on configuring the RingBuffer.*

