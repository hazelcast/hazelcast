/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.impl.execution.OutboundCollector;
import com.hazelcast.jet.impl.execution.OutboxImpl;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.test.JetAssert.assertSame;
import static com.hazelcast.util.Preconditions.checkNotNegative;

/**
 * {@code Outbox} implementation suitable to be used in tests.
 */
public final class TestOutbox implements Outbox {

    private static final SerializationService IDENTITY_SERIALIZER = new MockSerializationService();

    private final Queue<Object>[] buckets;
    private final Queue<Entry<MockData, MockData>> snapshotQueue = new ArrayDeque<>();
    private final OutboxImpl outbox;

    /** Items that were rejected for each output ordinal */
    private final Object[] rejectedItems;
    /** Rejected snapshot key */
    private Object rejectedSnapshotKey;
    /** Rejected snapshot value */
    private Object rejectedSnapshotValue;

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

        rejectedItems = new Object[edgeCapacities.length];
        allOrdinals = IntStream.range(0, edgeCapacities.length).toArray();

        OutboundCollector[] outstreams = new OutboundCollector[edgeCapacities.length + (snapshotCapacity > 0 ? 1 : 0)];
        Arrays.setAll(outstreams, i ->
                i < edgeCapacities.length
                    ? e -> addToQueue(buckets[i], edgeCapacities[i], e)
                    : e -> addToQueue(snapshotQueue, snapshotCapacity, (Entry<MockData, MockData>) e));

        outbox = new OutboxImpl(outstreams, snapshotCapacity > 0, new ProgressTracker(), IDENTITY_SERIALIZER,
                Integer.MAX_VALUE);
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
    public boolean offer(int[] ordinals, @Nonnull Object item) {
        boolean offerResult = outbox.offer(ordinals, item);
        for (int ordinal : ordinals) {
            rejectedItems[ordinal] = check(item, rejectedItems[ordinal], offerResult);
        }
        return offerResult;
    }

    @Override
    public boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
        boolean offerResult = outbox.offerToSnapshot(key, value);
        rejectedSnapshotKey = check(key, rejectedSnapshotKey, offerResult);
        rejectedSnapshotValue = check(value, rejectedSnapshotValue, offerResult);
        return offerResult;
    }

    @CheckReturnValue
    private Object check(Object item, Object rejectedItem, boolean offerResult) {
        if (rejectedItem != null) {
            assertSame("Different item provided after offer() was rejected", rejectedItem, item);
        }
        return offerResult ? null : item;
    }

    /**
     * Exposes individual output queues to the testing code.
     * @param ordinal ordinal of the bucket
     */
    public Queue<Object> queue(int ordinal) {
        return buckets[ordinal];
    }

    /**
     * Returns the queue to which snapshot is written.
     */
    public Queue<Entry<MockData, MockData>> snapshotQueue() {
        return snapshotQueue;
    }

    /**
     * Move all items from the queue to the {@code target} collection and make
     * the outbox available to accept more items. Also calls {@link
     * #reset()}. If you have a limited capacity outbox, you need to call
     * this regularly.
     *
     * @param queueOrdinal the queue from Outbox to drain
     * @param target target list
     * @param logItems whether to log drained items to {@code System.out}
     */
    public <T> void drainQueueAndReset(int queueOrdinal, Collection<T> target, boolean logItems) {
        drainInternal(queue(queueOrdinal), target, logItems);
    }

    /**
     * Move all items from the snapshot queue to the {@code target} collection
     * and make the outbox available to accept more items. Also calls {@link
     * #reset()}. If you have a limited capacity outbox, you need to call
     * this regularly.
     *
     * @param target target list
     * @param logItems whether to log drained items to {@code System.out}
     */
    public <T> void drainSnapshotQueueAndReset(Collection<T> target, boolean logItems) {
        drainInternal(snapshotQueue(), target, logItems);
    }

    private <T> void drainInternal(Queue<?> q, Collection<T> target, boolean logItems) {
        for (Object o; (o = q.poll()) != null; ) {
            target.add((T) o);
            if (logItems) {
                System.out.println(LocalTime.now() + " Output: " + o);
            }
        }
        reset();
    }

    /**
     * Call this method after any of the {@code offer()} methods returned
     * {@code false} to be able to offer again. Method is called automatically
     * from {@link #drainQueueAndReset} and {@link #drainSnapshotQueueAndReset}
     * methods.
     */
    public void reset() {
        outbox.reset();
    }

    @Override
    public String toString() {
        return Arrays.toString(buckets);
    }

    /**
     * Javadoc pending
     */
    public static class MockSerializationService implements SerializationService {

        @Override
        public <B extends Data> B toData(Object obj) {
            return (B) new MockData(obj);
        }

        @Override
        public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
            return (B) new MockData(obj);
        }

        @Override
        public <T> T toObject(Object data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T toObject(Object data, Class klazz) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManagedContext getManagedContext() {
            return o -> o; // initialize() will do nothing
        }
    }

    /**
     * Javadoc pending
     */
    public static class MockData implements Data {
        private final Object object;

        /**
         * Javadoc pending
         */
        public MockData(Object object) {
            this.object = object;
        }

        /**
         * Javadoc pending
         */
        public Object getObject() {
            return object;
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int totalSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyTo(byte[] dest, int destPos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int dataSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getHeapCost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionHash() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasPartitionHash() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash64() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPortable() {
            throw new UnsupportedOperationException();
        }
    }
}
