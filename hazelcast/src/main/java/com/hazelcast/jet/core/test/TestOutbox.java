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

package com.hazelcast.jet.core.test;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.impl.execution.OutboundCollector;
import com.hazelcast.jet.impl.execution.OutboxImpl;
import com.hazelcast.jet.impl.execution.OutboxInternal;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;

import javax.annotation.Nonnull;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.jet.Util.entry;

/**
 * {@code Outbox} implementation suitable to be used in tests.
 *
 * @since Jet 3.0
 */
public final class TestOutbox implements OutboxInternal {

    private final Queue<Object>[] buckets;
    private final Queue<Entry<Object, Object>> snapshotQueue = new ArrayDeque<>();
    private final OutboxImpl outbox;
    private final SerializationService serializationService;

    private final int[] allOrdinals;

    /**
     * @param capacities Capacities of individual buckets. Number of buckets
     *                   is determined by the number of provided capacities.
     *                   There is no snapshot bucket.
     */
    public TestOutbox(int ... capacities) {
        this(capacities, 0);
    }

    /**
     * @param edgeCapacities Capacities of individual buckets. Number of buckets
     *                      is determined by the number of provided capacities.
     * @param snapshotCapacity Capacity of snapshot bucket. If 0, snapshot queue
     *                         is not present.
     */
    public TestOutbox(int[] edgeCapacities, int snapshotCapacity) {
        checkNotNegative(snapshotCapacity, "snapshotCapacity must be >= 0 (0 for no snapshot queue)");

        buckets = new Queue[edgeCapacities.length];
        Arrays.setAll(buckets, i -> new ArrayDeque());

        allOrdinals = IntStream.range(0, edgeCapacities.length).toArray();

        OutboundCollector[] outstreams = new OutboundCollector[edgeCapacities.length + (snapshotCapacity > 0 ? 1 : 0)];
        Arrays.setAll(outstreams, i ->
                i < edgeCapacities.length
                    ? e -> addToQueue(buckets[i], edgeCapacities[i], e)
                    : e -> addToQueue(snapshotQueue, snapshotCapacity, deserializeSnapshotEntry((Entry<Data, Data>) e)));

        serializationService = new DefaultSerializationServiceBuilder().build();
        outbox = new OutboxImpl(outstreams, snapshotCapacity > 0, new ProgressTracker(), serializationService,
                Integer.MAX_VALUE, new AtomicLongArray(outstreams.length + (snapshotCapacity > 0 ? 1 : 0)));
        outbox.reset();
    }

    private static <E> ProgressState addToQueue(Queue<? super E> queue, int capacity, E o) {
        if (capacity > queue.size()) {
            queue.offer(o);
            return ProgressState.DONE;
        } else {
            return ProgressState.NO_PROGRESS;
        }
    }

    @Override
    public int bucketCount() {
        return outbox.bucketCount();
    }

    @Override
    public boolean offer(int ordinal, @Nonnull Object item) {
        return offer(ordinal == -1 ? allOrdinals : new int[]{ordinal}, item);
    }

    @Override
    public boolean offer(@Nonnull Object item) {
        return offer(allOrdinals, item);
    }

    @Override
    public boolean offer(@Nonnull int[] ordinals, @Nonnull Object item) {
        return outbox.offer(ordinals, item);
    }

    @Override
    public boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
        return outbox.offerToSnapshot(key, value);
    }

    /**
     * Exposes individual output queues to the testing code.
     * @param ordinal ordinal of the bucket
     */
    public <T> Queue<T> queue(int ordinal) {
        return (Queue<T>) buckets[ordinal];
    }

    /**
     * Returns the queue to which snapshot is written. It contains serialized
     * data, if you need deserialized data, you might prefer to use
     * {@link #drainSnapshotQueueAndReset}.
     */
    public Queue<Entry<Object, Object>> snapshotQueue() {
        return snapshotQueue;
    }

    /**
     * Move all items from the queue to the {@code target} collection and make
     * the outbox available to accept more items. Also calls {@link
     * #reset()}. If you have a limited capacity outbox, you need to call
     * this method regularly.
     *
     * @param queueOrdinal the queue from Outbox to drain
     * @param target target collection
     * @param logItems whether to log drained items to {@code System.out}
     */
    public <T> void drainQueueAndReset(int queueOrdinal, Collection<T> target, boolean logItems) {
        drainInternal(queue(queueOrdinal), target, logItems, "Output-" + queueOrdinal);
    }

    /**
     * Move all items from all queues (except the snapshot queue) to the {@code
     * target} list of collections. Queue N is moved to collection at target N
     * etc. Also calls {@link #reset()}. If you have a limited capacity outbox,
     * you need to call this method regularly.
     *
     * @param target list of target collections
     * @param logItems whether to log drained items to {@code System.out}
     */
    public <T> void drainQueuesAndReset(List<? extends Collection<T>> target, boolean logItems) {
        for (int ordinal : allOrdinals) {
            drainQueueAndReset(ordinal, target.get(ordinal), logItems);
        }
    }

    /**
     * Deserialize and move all items from the snapshot queue to the {@code
     * target} collection and make the outbox available to accept more items.
     * Also calls {@link #reset()}. If you have a limited capacity outbox, you
     * need to call this method regularly.
     *
     * @param target target list
     * @param logItems whether to log drained items to {@code System.out}
     */
    @SuppressWarnings("unchecked")
    public <K, V> void drainSnapshotQueueAndReset(Collection<? super Entry<K, V>> target, boolean logItems) {
        drainInternal(snapshotQueue(), (Collection) target, logItems, "Output-ss");
    }

    private <K, V> Entry<K, V> deserializeSnapshotEntry(Entry<Data, Data> t) {
        return entry(serializationService.toObject(t.getKey()), serializationService.toObject(t.getValue()));
    }

    private <T> void drainInternal(Queue<? extends T> q, Collection<? super T> target,
                                      boolean logItems, String prefix) {
        for (T o; (o = q.poll()) != null; ) {
            target.add(o);
            if (logItems) {
                System.out.println(LocalTime.now() + " " + prefix + ": " + o);
            }
        }
        reset();
    }

    @Override
    public void reset() {
        outbox.reset();
    }

    @Override
    public boolean hasUnfinishedItem() {
        return outbox.hasUnfinishedItem();
    }

    @Override
    public void block() {
        outbox.block();
    }

    @Override
    public void unblock() {
        outbox.unblock();
    }

    @Override
    public long lastForwardedWm() {
        return outbox.lastForwardedWm();
    }

    @Override
    public String toString() {
        return Arrays.toString(buckets);
    }
}
