package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

import java.util.List;

@GenerateCodec(id = TemplateConstants.RINGBUFFER_TEMPLATE_ID, name = "Ringbuffer", ns = "Hazelcast.Client.Protocol.Ringbuffer")
public interface RingbufferCodecTemplate {
    /**
     * Returns number of items in the ringbuffer. If no ttl is set, the size will always be equal to capacity after the
     * head completed the first looparound the ring. This is because no items are getting retired.
     *
     * @param name Name of the Ringbuffer
     * @return the size
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.LONG)
    Object size(String name);

    /**
     * Returns the sequence of the tail. The tail is the side of the ringbuffer where the items are added to.
     * The initial value of the tail is -1.
     *
     * @param name Name of the Ringbuffer
     * @return the sequence of the tail
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.LONG)
    Object tailSequence(String name);
    /**
     * Returns the sequence of the head. The head is the side of the ringbuffer where the oldest items in the ringbuffer
     * are found. If the RingBuffer is empty, the head will be one more than the tail.
     * The initial value of the head is 0 (1 more than tail).
     *
     * @param name Name of the Ringbuffer
     * @return the sequence of the head
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.LONG)
    Object headSequence(String name);
    /**
     * Returns the capacity of this Ringbuffer.
     *
     * @param name Name of the Ringbuffer
     * @return the capacity
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.LONG)
    Object capacity(String name);

    /**
     * Returns the remaining capacity of the ringbuffer. The returned value could be stale as soon as it is returned.
     * If ttl is not set, the remaining capacity will always be the capacity.
     *
     * @param name Name of the Ringbuffer
     * @return the remaining capacity
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.LONG)
    Object remainingCapacity(String name);

    /**
     * Adds an item to the tail of the Ringbuffer. If there is no space in the Ringbuffer, the add will overwrite the oldest
     * item in the ringbuffer no matter what the ttl is. For more control on this behavior, check the
     * addAsync(Object, OverflowPolicy) and the  OverflowPolicy. The returned value is the sequence of the added item.
     * Using this sequence you can read the added item.
     * This sequence will always be unique for this Ringbuffer instance so it can be used as a unique id generator if you are
     * publishing items on this Ringbuffer. However you need to take care of correctly determining an initial id when any node
     * uses the ringbuffer for the first time. The most reliable way to do that is to write a dummy item into the ringbuffer and
     * use the returned sequence as initial  id. On the reading side, this dummy item should be discard. Please keep in mind that
     * this id is not the sequence of the item you are about to publish but from a previously published item. So it can't be used
     * to find that item.
     *
     * @param name Name of the Ringbuffer
     * @param overflowPolicy the OverflowPolicy to use.
     * @param value to item to add
     * @return the sequence of the added item.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.LONG)
    Object add(String name, int overflowPolicy, Data value);

    /**
     * Asynchronously writes an item with a configurable OverflowPolicy. If there is space in the ringbuffer, the call
     * will return the sequence of the written item. If there is no space, it depends on the overflow policy what happens:
     * OverflowPolicy OVERWRITE we just overwrite the oldest item in the ringbuffer and we violate the ttl
     * OverflowPolicy FAIL we return -1. The reason that FAIL exist is to give the opportunity to obey the ttl. If
     * blocking behavior is required, this can be implemented using retrying in combination with a exponential backoff.
     *
     * @param name Name of the Ringbuffer
     * @param overflowPolicy the OveflowPolicy to use
     * @param value to item to add
     * @return the sequenceId of the added item, or -1 if the add failed.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.LONG)
    Object addAsync(String name, int overflowPolicy, Data value);
    /**
     * Reads one item from the Ringbuffer. If the sequence is one beyond the current tail, this call blocks until an
     * item is added. This method is not destructive unlike e.g. a queue.take. So the same item can be read by multiple
     * readers or it can be read multiple times by the same reader. Currently it isn't possible to control how long this
     * call is going to block. In the future we could add e.g. tryReadOne(long sequence, long timeout, TimeUnit unit).
     *
     * @param name Name of the Ringbuffer
     * @param sequence the sequence of the item to read.
     * @return the read item
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.DATA)
    Object readOne(String name, long sequence);

    /**
     * Adds all the items of a collection to the tail of the Ringbuffer. A addAll is likely to outperform multiple calls
     * to add(Object) due to better io utilization and a reduced number of executed operations. If the batch is empty,
     * the call is ignored. When the collection is not empty, the content is copied into a different data-structure.
     * This means that: after this call completes, the collection can be re-used. the collection doesn't need to be serializable.
     * If the collection is larger than the capacity of the ringbuffer, then the items that were written first will be
     * overwritten. Therefor this call will not block. The items are inserted in the order of the Iterator of the collection.
     * If an addAll is executed concurrently with an add or addAll, no guarantee is given that items are contiguous.
     * The result of the future contains the sequenceId of the last written item
     *
     * @param name Name of the Ringbuffer
     * @param valueList the batch of items to add
     * @param overflowPolicy the overflowPolicy to use
     * @return the ICompletableFuture to synchronize on completion.
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.LONG)
    Object addAllAsync(String name, List<Data> valueList, int overflowPolicy);

    /**
     * Reads a batch of items from the Ringbuffer. If the number of available items after the first read item is smaller
     * than the maxCount, these items are returned. So it could be the number of items read is smaller than the maxCount.
     * If there are less items available than minCount, then this call blacks. Reading a batch of items is likely to
     * perform better because less overhead is involved. A filter can be provided to only select items that need to be read.
     * If the filter is null, all items are read. If the filter is not null, only items where the filter function returns
     * true are returned. Using filters is a good way to prevent getting items that are of no value to the receiver.
     * This reduces the amount of IO and the number of operations being executed, and can result in a significant performance improvement.
     *
     * @param name Name of the Ringbuffer
     * @param startSequence the startSequence of the first item to read
     * @param minCount the minimum number of items to read.
     * @param maxCount the maximum number of items to read.
     * @param filter Filter is allowed to be null, indicating there is no filter.
     * @return  a future containing the items read.
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.READ_RESULT_SET)
    Object readManyAsync(String name, long startSequence, int minCount, int maxCount, @Nullable Data filter);
}
