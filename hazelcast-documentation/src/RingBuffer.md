## RingBuffer


The RingBuffer is a data structure - developed by Hazelcast - where the data is stored in a ring-like structure. You can think of it as a circular array with a certain capacity. In this circular array, the oldest item gets overwritten in case a new item is written when the maximum capacity is reached. For now, the RingBuffer is not a partitioned data structure; its data is stored in a single partition and the replicas are stored in another partition.

The RingBuffer can be used in the applications where queue's are used.  Unlike the queues, the RingBuffer does not remove the items, it only reads the items at their locations. By this way, you can read the same item multiple times.

Each element in a RingBuffer can be accessed using a sequence ID. This ID is between the head and tail (inclusive) of the RingBuffer. Head is the side where items are discarded and tail is the side where items are added to.

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



***RELATED INFORMATION***

*Please refer to the [RingBuffer Configuration section](#ringbuffer-configuration) for more information on configuring the RingBuffer.*

