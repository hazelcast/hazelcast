/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.ringbuffer;

import com.hazelcast.collection.BaseQueue;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IFunction;
import com.hazelcast.topic.ITopic;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * A Ringbuffer is a data structure where the content is stored in a ring-like
 * structure. A ringbuffer has a fixed capacity so it won't grow beyond
 * that capacity and endanger the stability of the system. If that capacity
 * is exceeded, the oldest item in the ringbuffer is overwritten.
 * <p>
 * The ringbuffer has 2 always incrementing sequences:
 * <ol>
 * <li>
 * {@link #tailSequence()}: this is the side where the youngest item is found.
 * So the tail is the side of the ringbuffer where items are added to.
 * </li>
 * <li>
 * {@link #headSequence()}: this is the side where the oldest items are found.
 * So the head is the side where items get discarded.
 * </li>
 * </ol>
 * The items in the ringbuffer can be found by a sequence that is in between
 * (inclusive) the head and tail sequence.
 * <p>
 * If data is read from a ringbuffer with a sequence that is smaller than the
 * headSequence, it means that the data is not available anymore and a
 * {@link StaleSequenceException} is thrown.
 * <p>
 * A Ringbuffer currently is a replicated, but not partitioned data structure.
 * So all data is stored in a single partition, similarly to the {@link
 * IQueue} implementation.
 * <p>
 * A Ringbuffer can be used in a way similar to the IQueue, but one of the key
 * differences is that a {@code queue.take} is destructive, meaning that only 1
 * thread is able to take an item. A {@code ringbuffer.read} is not
 * destructive, so you can have multiple threads reading the same item multiple
 * times.
 * <p>
 * The Ringbuffer is the backing data structure for the reliable
 * {@link ITopic} implementation. See
 * {@link com.hazelcast.config.ReliableTopicConfig}.
 * <p>
 * A Ringbuffer can be configured to be backed by a
 * {@link RingbufferStore}. All write methods will delegate
 * to the store to persist the items, while reader methods will try to read
 * items from the store if not found in the in-memory Ringbuffer.
 * <p>
 * When a Ringbuffer is constructed with a backing store, head and tail
 * sequences are set to the following
 * <ul>
 * <li>{@code tailSequence}: {@code lastStoreSequence}</li>
 * <li>{@code headSequence}: {@code lastStoreSequence} + 1</li>
 * </ul>
 * where {@code lastStoreSequence} is the sequence of the previously last
 * stored item.
 * <p>
 * Supports split brain protection {@link SplitBrainProtectionConfig} since 3.10 in
 * cluster versions 3.10 and higher.
 * <p>
 * <b>Asynchronous methods</b>
 * <p>
 * Asynchronous methods return a {@link CompletionStage} that can be used to
 * chain further computation stages. Alternatively, a {@link java.util.concurrent.CompletableFuture}
 * can be obtained via {@link CompletionStage#toCompletableFuture()} to wait
 * for the operation to complete in a blocking way.
 * <p>
 * Actions supplied for dependent completions of default non-async methods and async methods
 * without an explicit {@link java.util.concurrent.Executor} argument are performed
 * by the {@link java.util.concurrent.ForkJoinPool#commonPool()} (unless it does not
 * support a parallelism level of at least 2, in which case a new {@code Thread} is
 * created per task).
 *
 * @param <E> The type of the elements that the Ringbuffer contains
 * @since 3.5
 */
public interface Ringbuffer<E> extends DistributedObject {

    /**
     * Returns the capacity of this Ringbuffer.
     *
     * @return the capacity.
     */
    long capacity();

    /**
     * Returns number of items in the Ringbuffer.
     * <p>
     * If no TTL is set, the size will always be equal to capacity after the
     * head completed the first loop around the ring. This is because no items
     * are being removed.
     *
     * @return the size.
     */
    long size();

    /**
     * Returns the sequence of the tail. The tail is the side of the Ringbuffer
     * where the items are added to.
     * <p>
     * The initial value of the tail is -1 if the Ringbuffer is not backed by a
     * store, otherwise tail sequence will be set to the sequence of the
     * previously last stored item.
     *
     * @return the sequence of the tail.
     */
    long tailSequence();

    /**
     * Returns the sequence of the head. The head is the side of the Ringbuffer
     * where the oldest items in the Ringbuffer are found.
     * <p>
     * If the RingBuffer is empty, the head will be one more than the tail.
     * <p>
     * The initial value of the head is 0 if the Ringbuffer is not backed by a
     * store, otherwise head sequence will be set to the sequence of the
     * previously last stored item + 1. In both cases head sequence is 1 more
     * than the tail sequence.
     *
     * @return the sequence of the head.
     */
    long headSequence();

    /**
     * Returns the remaining capacity of the ringbuffer. If TTL is enabled,
     * then the returned capacity is equal to the total capacity of the
     * ringbuffer minus the number of used slots in the ringbuffer which have
     * not yet been marked as expired and cleaned up. Keep in mind that some
     * slots could have expired items that have not yet been cleaned up and
     * that the returned value could be stale as soon as it is returned.
     * <p>
     * If TTL is disabled, the remaining capacity is equal to the total
     * ringbuffer capacity.
     *
     * @return the remaining capacity
     * @see com.hazelcast.config.RingbufferConfig#DEFAULT_TTL_SECONDS
     * @see #capacity()
     */
    long remainingCapacity();

    /**
     * Adds an item to the tail of the Ringbuffer. If there is no space in the
     * Ringbuffer, the add will overwrite the oldest item in the ringbuffer no
     * matter what the TTL is. For more control on this behavior, check the
     * {@link #addAsync(Object, OverflowPolicy)} and the {@link OverflowPolicy}.
     * <p>
     * The returned value is the sequence of the added item. Using this sequence
     * you can read the added item.
     *
     * <h3>Using the sequence as ID</h3>
     * This sequence will always be unique for this Ringbuffer instance so it
     * can be used as a unique ID generator if you are publishing items on this
     * Ringbuffer. However you need to take care of correctly determining an
     * initial ID when any node uses the Ringbuffer for the first time. The
     * most reliable way to do that is to write a dummy item into the Ringbuffer
     * and use the returned sequence as initial ID. On the reading side, this
     * dummy item should be discard. Please keep in mind that this ID is not the
     * sequence of the item you are about to publish but from a previously
     * published item. So it can't be used to find that item.
     * <p>
     * If the Ringbuffer is backed by a {@link RingbufferStore},
     * the item gets persisted by the underlying store via
     * {@link RingbufferStore#store(long, Object)}. Note that
     * in case an exception is thrown by the store, it prevents the item from being
     * added to the Ringbuffer, keeping the store, primary and the backups
     * consistent.
     *
     * @param item the item to add.
     * @return the sequence of the added item.
     * @throws NullPointerException if item is null.
     * @see #addAsync(Object, OverflowPolicy)
     */
    long add(@Nonnull E item);

    /**
     * Asynchronously writes an item with a configurable {@link OverflowPolicy}.
     * <p>
     * If there is space in the Ringbuffer, the call will return the sequence
     * of the written item. If there is no space, it depends on the overflow
     * policy what happens:
     * <ol>
     * <li>{@link OverflowPolicy#OVERWRITE}: we just overwrite the oldest item
     * in the Ringbuffer and we violate the TTL</li>
     * <li>{@link OverflowPolicy#FAIL}: we return -1 </li>
     * </ol>
     * <p>
     * The reason that FAIL exist is to give the opportunity to obey the TTL.
     * If blocking behavior is required, this can be implemented using retrying
     * in combination with an exponential backoff. Example:
     * <pre>{@code
     * long sleepMs = 100;
     * for (; ; ) {
     *   long result = ringbuffer.addAsync(item, FAIL).toCompletableFuture().get();
     *   if (result != -1) {
     *     break;
     *   }
     *   TimeUnit.MILLISECONDS.sleep(sleepMs);
     *   sleepMs = min(5000, sleepMs * 2);
     * }
     * }</pre>
     * <p>
     * If the Ringbuffer is backed by a {@link RingbufferStore},
     * the item gets persisted by the underlying store via
     * {@link RingbufferStore#store(long, Object)}. Note
     * that in case an exception is thrown by the store, it prevents the item
     * from being added to the Ringbuffer, keeping the store, primary and the
     * backups consistent.
     *
     * @param item           the item to add
     * @param overflowPolicy the OverflowPolicy to use.
     * @return the sequenceId of the added item, or -1 if the add failed.
     * @throws NullPointerException if item or overflowPolicy is null.
     */
    CompletionStage<Long> addAsync(@Nonnull E item, @Nonnull OverflowPolicy overflowPolicy);

    /**
     * Reads one item from the Ringbuffer.
     * <p>
     * If the sequence is one beyond the current tail, this call blocks until
     * an item is added. This means that the ringbuffer can be processed using
     * the following idiom:
     * <pre>{@code
     * Ringbuffer<String> ringbuffer = hz.getRingbuffer("rb");
     * long seq = ringbuffer.headSequence();
     * while(true){
     *   String item = ringbuffer.readOne(seq);
     *   seq++;
     *   ... process item
     * }
     * }</pre>
     * <p>
     * This method is not destructive unlike e.g. a {@link BaseQueue#take()}.
     * So the same item can be read by multiple readers or it can be read
     * multiple times by the same reader.
     * <p>
     * Currently it isn't possible to control how long this call is going to
     * block. In the future we could add e.g.
     * {@code tryReadOne(long sequence, long timeout, TimeUnit unit)}.
     * <p>
     * If the item is not in the Ringbuffer an attempt is made to read it from
     * the underlying {@link RingbufferStore} via
     * {@link RingbufferStore#load(long)} if store is
     * configured for the Ringbuffer. These cases may increase the execution time
     * significantly depending on the implementation of the store. Note that
     * exceptions thrown by the store are propagated to the caller.
     *
     * @param sequence the sequence of the item to read.
     * @return the read item
     * @throws StaleSequenceException   if the sequence is smaller than
     *                                  {@link #headSequence()}. Because a
     *                                  Ringbuffer won't store all event
     *                                  indefinitely, it can be that the data
     *                                  for the given sequence doesn't exist
     *                                  anymore and the
     *                                  {@link StaleSequenceException} is thrown.
     *                                  It is up to the caller to deal with
     *                                  this particular situation, e.g. throw an
     *                                  Exception or restart from the last known
     *                                  head. That is why the
     *                                  StaleSequenceException contains the last
     *                                  known head.
     * @throws IllegalArgumentException if sequence is smaller than 0 or larger than {@link #tailSequence()}+1.
     * @throws InterruptedException     if the call is interrupted while blocking.
     */
    E readOne(long sequence) throws InterruptedException;

    /**
     * Adds all the items of a collection to the tail of the Ringbuffer.
     * <p>
     * An addAll is likely to outperform multiple calls to {@link #add(Object)}
     * due to better io utilization and a reduced number of executed operations.
     * If the batch is empty, the call is ignored.
     * <p>
     * When the collection is not empty, the content is copied into a different
     * data-structure. This means that:
     * <ol>
     * <li>after this call completes, the collection can be re-used.</li>
     * <li>the collection doesn't need to be serializable</li>
     * </ol>
     * <p>
     * If the collection is larger than the capacity of the Ringbuffer, then
     * the items that were written first will be overwritten. Therefore this
     * call will not block.
     * <p>
     * The items are inserted in the order of the Iterator of the collection.
     * If an addAll is executed concurrently with an add or addAll, no
     * guarantee is given that items are contiguous.
     * <p>
     * The result of the future contains the sequenceId of the last written
     * item.
     * <p>
     * If the Ringbuffer is backed by a {@link RingbufferStore},
     * the items are persisted by the underlying store via
     * {@link RingbufferStore#storeAll(long, Object[])}.
     * Note that in case an exception is thrown by the store, it makes the
     * Ringbuffer not adding any of the items to the primary and the backups.
     * Keeping the store consistent with the primary and the backups is the
     * responsibility of the store.
     *
     * @param collection the batch of items to add.
     * @return the CompletionStage to synchronize on completion.
     * @throws NullPointerException     if batch is null, or if an item in this
     *                                  batch is null or if overflowPolicy is null
     * @throws IllegalArgumentException if collection is empty
     */
    CompletionStage<Long> addAllAsync(@Nonnull Collection<? extends E> collection,
                                      @Nonnull OverflowPolicy overflowPolicy);

    /**
     * Reads a batch of items from the Ringbuffer. If the number of available
     * items after the first read item is smaller than the {@code maxCount},
     * these items are returned. So it could be the number of items read is
     * smaller than the {@code maxCount}.
     * <p>
     * If there are less items available than {@code minCount}, then this call
     * blocks.
     * <p>
     * <b>Warning:</b>
     * <p>
     * These blocking calls consume server memory and if there are many calls,
     * it can be possible to see leaking memory or OOME.
     * <p>
     * Reading a batch of items is likely to perform better because less
     * overhead is involved.
     * <p>
     * A filter can be provided to only select items that need to be read. If the
     * filter is null, all items are read. If the filter is not null, only items
     * where the filter function returns true are returned. Using filters is a
     * good way to prevent getting items that are of no value to the receiver.
     * This reduces the amount of IO and the number of operations being executed,
     * and can result in a significant performance improvement.
     * <p>
     * For each item not available in the Ringbuffer an attempt is made to read
     * it from the underlying {@link RingbufferStore} via
     * multiple invocations of {@link RingbufferStore#load(long)},
     * if store is configured for the Ringbuffer. These cases may increase the
     * execution time significantly depending on the implementation of the store.
     * Note that exceptions thrown by the store are propagated to the caller.
     * <p>
     * If the startSequence is smaller than the smallest sequence still available
     * in the Ringbuffer ({@link #headSequence()}, then the smallest available
     * sequence will be used as the start sequence and the minimum/maximum
     * number of items will be attempted to be read from there on.
     * <p>
     * If the startSequence is bigger than the last available sequence in the
     * Ringbuffer ({@link #tailSequence()}), then the last available sequence
     * plus one will be used as the start sequence and the call will block
     * until further items become available and it can read at least the
     * minimum number of items.
     *
     * @param startSequence the startSequence of the first item to read.
     * @param minCount      the minimum number of items to read.
     * @param maxCount      the maximum number of items to read.
     * @param filter        the filter. Filter is allowed to be null, indicating
     *                      there is no filter.
     * @return a future containing the items read.
     * @throws IllegalArgumentException if startSequence is smaller than 0
     *                                  or if minCount smaller than 0
     *                                  or if minCount larger than maxCount,
     *                                  or if maxCount larger than the capacity of the ringbuffer
     *                                  or if maxCount larger than 1000 (to prevent overload)
     */
    CompletionStage<ReadResultSet<E>> readManyAsync(long startSequence, int minCount, int maxCount,
                                                    @Nullable IFunction<E, Boolean> filter);
}
