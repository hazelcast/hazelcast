/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.serialization.SerializableByConvention;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * ConcurrentHashMap to extend iterator capability.
 *
 * @param <K> Type of the key
 * @param <V> Type of the value
 */
@SerializableByConvention
public class SampleableConcurrentHashMap<K, V> extends ConcurrentReferenceHashMap<K, V> {

    private static final float LOAD_FACTOR = 0.91f;

    public SampleableConcurrentHashMap(int initialCapacity) {
        this(initialCapacity, ReferenceType.STRONG, ReferenceType.STRONG);
    }

    public SampleableConcurrentHashMap(int initialCapacity, ReferenceType keyType, ReferenceType valueType) {
        // Concurrency level 1 is important for fetch-method to function properly.
        // Moreover partitions are single threaded and higher concurrency has not much gain
        this(initialCapacity, LOAD_FACTOR, 1, keyType, valueType, null);
    }

    private SampleableConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel,
                                        ReferenceType keyType, ReferenceType valueType, EnumSet<Option> options) {
        super(initialCapacity, loadFactor, concurrencyLevel, keyType, valueType, options);
    }

    /**
     * Fetches keys from given <code>tableIndex</code> as <code>size</code>
     * and puts them into <code>keys</code> list.
     *
     * @param tableIndex Index (checkpoint) for starting point of fetch operation
     * @param size       Count of how many keys will be fetched
     * @param keys       List that fetched keys will be put into
     * @return the next index (checkpoint) for later fetches
     */
    public int fetchKeys(int tableIndex, int size, List<K> keys) {
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
                    if (isValidForFetching(value, now)) {
                        keys.add(nextEntry.key());
                        counter++;
                    }
                }
                nextEntry = nextEntry.next;
            }
        }
        return nextTableIndex;
    }

    /**
     * Fetches entries from given <code>tableIndex</code> as <code>size</code>
     * and puts them into <code>entries</code> list.
     *
     * @param tableIndex Index (checkpoint) for starting point of fetch operation
     * @param size       Count of how many entries will be fetched
     * @param entries    List that fetched entries will be put into
     * @return the next index (checkpoint) for later fetches
     */
    public int fetchEntries(int tableIndex, int size, List<Map.Entry<K, V>> entries) {
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
                    if (isValidForFetching(value, now)) {
                        K key = nextEntry.key();
                        entries.add(new AbstractMap.SimpleEntry<K, V>(key, value));
                        counter++;
                    }
                }
                nextEntry = nextEntry.next;
            }
        }
        return nextTableIndex;
    }

    protected boolean isValidForFetching(V value, long now) {
        if (value instanceof Expirable) {
            return !((Expirable) value).isExpiredAt(now);
        }
        return true;
    }

    /**
     * Entry to define keys and values for sampling.
     *
     * @param <K> type of key
     * @param <V> type of value
     */
    public static class SamplingEntry<K, V> {

        protected final K key;
        protected final V value;

        public SamplingEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getEntryKey() {
            return key;
        }

        public V getEntryValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SamplingEntry)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            SamplingEntry e = (SamplingEntry) o;
            return Objects.equals(key, e.key) && Objects.equals(value, e.value);
        }

        @Override
        public int hashCode() {
            return (key == null ? 0 : key.hashCode())
                    ^ (value == null ? 0 : value.hashCode());
        }

        public String toString() {
            return key + "=" + value;
        }
    }

    protected <E extends SamplingEntry> E createSamplingEntry(K key, V value) {
        return (E) new SamplingEntry(key, value);
    }

    /**
     * Gets and returns samples as <code>sampleCount</code>.
     *
     * @param sampleCount Count of samples
     * @return the sampled {@link SamplingEntry} list
     */
    public <E extends SamplingEntry> Iterable<E> getRandomSamples(int sampleCount) {
        if (sampleCount < 0) {
            throw new IllegalArgumentException("Sample count cannot be a negative value.");
        }
        if (sampleCount == 0 || size() == 0) {
            return Collections.EMPTY_LIST;
        }
        return new LazySamplingEntryIterableIterator<>(sampleCount);
    }

    /**
     * Not thread safe
     */
    private final class LazySamplingEntryIterableIterator<E extends SamplingEntry> implements Iterable<E>, Iterator<E> {

        private final int maxEntryCount;
        private final int randomNumber;
        private final int firstSegmentIndex;
        private int currentSegmentIndex;
        private int currentBucketIndex;
        private HashEntry<K, V> mostRecentlyReturnedEntry;
        private int returnedEntryCount;
        private boolean reachedToEnd;
        private E currentSample;

        private LazySamplingEntryIterableIterator(int maxEntryCount) {
            this.maxEntryCount = maxEntryCount;
            this.randomNumber = ThreadLocalRandomProvider.get().nextInt(Integer.MAX_VALUE);
            this.firstSegmentIndex = randomNumber % segments.length;
            this.currentSegmentIndex = firstSegmentIndex;
            this.currentBucketIndex = -1;
        }

        @Override
        public Iterator<E> iterator() {
            return this;
        }

        /**
         * Originally taken by Jaromir Hamala's implementation and changed as incremental implementation.
         * So kudos to Jaromir :)
         */
        @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
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
                        if (mostRecentlyReturnedEntry == null) {
                            mostRecentlyReturnedEntry = table[currentBucketIndex];
                        } else {
                            mostRecentlyReturnedEntry = mostRecentlyReturnedEntry.next;
                        }

                        while (mostRecentlyReturnedEntry != null) {
                            V value = mostRecentlyReturnedEntry.value();
                            K key = mostRecentlyReturnedEntry.key();

                            if (isValidForSampling(key, value)) {
                                currentSample = createSamplingEntry(key, value);
                                // If we reached end of entries, advance current bucket index
                                returnedEntryCount++;
                                return;
                            }
                            mostRecentlyReturnedEntry = mostRecentlyReturnedEntry.next;
                        }
                        // Advance current bucket index
                        currentBucketIndex = ++currentBucketIndex < table.length ? currentBucketIndex : 0;
                    } while (currentBucketIndex != firstBucketIndex);
                }
                // Advance current segment index
                currentSegmentIndex = ++currentSegmentIndex < segments.length ? currentSegmentIndex : 0;
                // Clear current bucket index to initialize at next segment
                currentBucketIndex = -1;
                // Clear current entry index to initialize at next segment
                mostRecentlyReturnedEntry = null;
            } while (currentSegmentIndex != firstSegmentIndex);

            reachedToEnd = true;
            currentSample = null;
        }

        @Override
        public boolean hasNext() {
            if (currentSample == null) {
                iterate();
            }
            return currentSample != null;
        }

        @Override
        public E next() {
            if (hasNext()) {
                E returnValue = currentSample;
                currentSample = null;
                return returnValue;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing is not supported");
        }
    }

    protected boolean isValidForSampling(K key, V value) {
        return key != null && value != null;
    }
}
