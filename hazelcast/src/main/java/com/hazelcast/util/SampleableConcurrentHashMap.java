/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.cache.impl.record.Expirable;
import com.hazelcast.nio.serialization.Data;

import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * ConcurrentHashMap to extend iterator capability.
 *
 * @param <K> Type of the key
 * @param <V> Type of the value
 */
public class SampleableConcurrentHashMap<K, V>
        extends ConcurrentReferenceHashMap<K, V> {

    private static final float LOAD_FACTOR = 0.91f;

    // Because of JDK6 compatibility,
    // we cannot use "java.util.concurrent.ThreadLocalRandom" (valid for JDK7+ versions).
    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM =
        new ThreadLocal<Random>() {
            @Override
            protected Random initialValue() {
                return new Random();
            }
        };

    public SampleableConcurrentHashMap(int initialCapacity) {
        // Concurrency level 1 is important for fetch-method to function properly.
        // Moreover partitions are single threaded and higher concurrency has not much gain
        this(initialCapacity, LOAD_FACTOR, 1, ReferenceType.STRONG, ReferenceType.STRONG, null);
    }

    public SampleableConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
            ReferenceType keyType, ReferenceType valueType, EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

    /**
     * Fetches keys from given <code>tableIndex</code> as <code>size</code>
     * and puts them into <code>keys</code> list.
     *
     * @param tableIndex    Index (checkpoint) for starting point of fetch operation
     * @param size          Count of how many keys will be fetched
     * @param keys          List that fetched keys will be put into
     *
     * @return the next index (checkpoint) for later fetches
     */
    public int fetch(int tableIndex, int size, List<Data> keys) {
        final long now = Clock.currentTimeMillis();
        final Segment<K, V> segment = segments[0];
        final HashEntry<K, V>[] currentTable = segment.table;
        int nextTableIndex;
        if (tableIndex >= 0 && tableIndex < segment.table.length) {
            nextTableIndex = tableIndex;
        } else {
            nextTableIndex = currentTable.length - 1;
        }
        int counter = 0;
        while (nextTableIndex >= 0 && counter < size) {
            HashEntry<K, V> nextEntry = currentTable[nextTableIndex--];
            while (nextEntry != null) {
                if (nextEntry.key() != null) {
                    final V value = nextEntry.value();
                    final boolean isExpired = (value instanceof Expirable)
                            && ((Expirable) value).isExpiredAt(now);
                    if (!isExpired) {
                        keys.add((Data) nextEntry.key());
                        counter++;
                    }
                }
                nextEntry = nextEntry.next;
            }
        }
        return nextTableIndex;
    }

    /**
     * Gets and returns samples as <code>sampleCount</code>.
     *
     * @param sampleCount Count of samples
     *
     * @return the sampled {@link com.hazelcast.util.ConcurrentReferenceHashMap.HashEntry} list
     */
    public Iterable<HashEntry<K, V>> samplingIterator(int sampleCount) {
        if (sampleCount < 0) {
            throw new IllegalArgumentException("Sample count cannot be a negative value.");
        }
        if (sampleCount == 0 || size() == 0) {
            return Collections.EMPTY_LIST;
        }

        return new LazySamplingEntryIterableIterator(sampleCount);
    }

    @Override
    Segment<K, V> newSegment(int initialCapacity, float lf, ReferenceType keyType, ReferenceType valueType,
                             boolean identityComparisons) {
        return new SampleableSegment<K, V>(initialCapacity, lf, keyType, valueType, identityComparisons);
    }

    static class SampleableSegment<K, V> extends Segment<K, V> {

        protected SampleableSegment(int initialCapacity, float lf, ReferenceType keyType, ReferenceType valueType,
                          boolean identityComparisons) {
            super(initialCapacity, lf, keyType, valueType, identityComparisons);
        }

        @Override
        protected HashEntry<K, V> newHashEntry(K key, int hash, HashEntry<K, V> next, V value) {
            return new IterableSamplingEntry<K, V>(key, hash, next, value, keyType, valueType, refQueue);
        }
    }

    /**
     * This class is implements both of "Iterable" and "Iterator" interfaces.
     * So we can use only one object (instead of two) both for "Iterable" and "Iterator" interfaces.
     *
     * NOTE: Assumed that it is not accessed by multiple threads. So there is no synchronization.
     */
    private final class LazySamplingEntryIterableIterator
            implements Iterable<HashEntry<K, V>>, Iterator<HashEntry<K, V>> {

        private final int maxEntryCount;
        private final int randomNumber;
        private final int firstSegmentIndex;
        private int currentSegmentIndex;
        private int currentBucketIndex;
        private HashEntry<K, V> currentEntry;
        private int returnedEntryCount;
        private boolean reachedToEnd;
        private HashEntry<K, V> currentSample;

        private LazySamplingEntryIterableIterator(int maxEntryCount) {
            this.maxEntryCount = maxEntryCount;
            this.randomNumber = THREAD_LOCAL_RANDOM.get().nextInt(Integer.MAX_VALUE);
            this.firstSegmentIndex = randomNumber % segments.length;
            this.currentSegmentIndex = firstSegmentIndex;
            this.currentBucketIndex = -1;
        }

        @Override
        public Iterator<HashEntry<K, V>> iterator() {
            return this;
        }

        /**
         * Originally taken by Jaromir Hamala's implementation and changed as incremental implementation.
         * So kudos to Jaromir :)
         */
        //CHECKSTYLE:OFF
        private void iterate() {
            if (returnedEntryCount >= maxEntryCount || reachedToEnd) {
                currentSample = null;
                return;
            }

            do {
                Segment<K, V> segment = segments[currentSegmentIndex];
                if (segment != null) {
                    HashEntry<K, V>[] table = segment.table;
                    // Pick up a starting point
                    int firstBucketIndex = randomNumber % table.length;
                    // If current bucket index is not initialized yet, initialize it with starting point
                    if (currentBucketIndex == -1) {
                        currentBucketIndex = firstBucketIndex;
                    }
                    do {
                        // If current entry is not initialized yet, initialize it
                        if (currentEntry == null) {
                            currentEntry = table[currentBucketIndex];
                        }
                        while (currentEntry != null) {
                            HashEntry<K, V> selectedEntry = currentEntry;
                            V value = selectedEntry.value();
                            // Advance to next entry
                            currentEntry = currentEntry.next;
                            if (value != null) {
                                currentSample = selectedEntry;
                                // If we reached end of entries, advance current bucket index
                                if (currentEntry == null) {
                                    currentBucketIndex = ++currentBucketIndex < table.length ? currentBucketIndex : 0;
                                }
                                returnedEntryCount++;
                                return;
                            }
                        }
                        // Advance current bucket index
                        currentBucketIndex = ++currentBucketIndex < table.length ? currentBucketIndex : 0;
                        // Clear current entry index to initialize at next bucket
                        currentEntry = null;
                    } while (currentBucketIndex != firstBucketIndex);
                }
                // Advance current segment index
                currentSegmentIndex = ++currentSegmentIndex < segments.length ? currentSegmentIndex : 0;
                // Clear current bucket index to initialize at next segment
                currentBucketIndex = -1;
                // Clear current entry index to initialize at next segment
                currentEntry = null;
            } while (currentSegmentIndex != firstSegmentIndex);

            reachedToEnd = true;
            currentSample = null;
        }
        //CHECKSTYLE:ON

        @Override
        public boolean hasNext() {
            iterate();
            return currentSample != null;
        }

        @Override
        public HashEntry<K, V> next() {
            if (currentSample != null) {
                return currentSample;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is not supported");
        }
    }

    /**
     * Iterable sampling entry to preventing from extra object creation for iteration.
     *
     * NOTE: Assumed that it is not accessed by multiple threads. So there is synchronization.
     */
    protected static class IterableSamplingEntry<K, V>
            extends HashEntry<K, V>
            implements Iterable<IterableSamplingEntry>, Iterator<IterableSamplingEntry> {

        private boolean iterated;

        protected IterableSamplingEntry(K key, int hash, HashEntry<K, V> next, V value, ReferenceType keyType,
                                        ReferenceType valueType, ReferenceQueue<Object> refQueue) {
            super(key, hash, next, value, keyType, valueType, refQueue);
        }

        @Override
        public Iterator<IterableSamplingEntry> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return !iterated;
        }

        @Override
        public IterableSamplingEntry next() {
            if (iterated) {
                throw new NoSuchElementException();
            }
            iterated = true;
            return this;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is supported");
        }
    }
}
