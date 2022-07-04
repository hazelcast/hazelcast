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

package com.hazelcast.internal.util;

import com.hazelcast.internal.eviction.Expirable;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.SerializableByConvention;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;

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
     * Fetches at least {@code size} keys from the given {@code pointers} and
     * puts them into the {@code keys} list.
     *
     * @param pointers the pointers defining the state where to begin iteration
     * @param size     Count of how many keys will be fetched
     * @param keys     List that fetched keys will be put into
     * @return the pointers defining the state where iteration has ended
     */
    public IterationPointer[] fetchKeys(IterationPointer[] pointers, int size, List<K> keys) {
        return fetchNext(pointers, size, (k, v) -> keys.add(k));
    }

    /**
     * Fetches at least {@code size} keys from the given {@code pointers} and
     * puts them into the {@code entries} list.
     *
     * @param pointers the pointers defining the state where to begin iteration
     * @param size     Count of how many entries will be fetched
     * @param entries  List that fetched entries will be put into
     * @return the pointers defining the state where iteration has ended
     */
    public IterationPointer[] fetchEntries(IterationPointer[] pointers,
                                           int size,
                                           List<Map.Entry<K, V>> entries) {
        return fetchNext(pointers, size, (k, v) -> entries.add(new AbstractMap.SimpleEntry<>(k, v)));
    }

    /**
     * Fetches at most {@code size} keys starting at the given {@code pointers} and
     * invokes the {@code entryConsumer} for each key-value pair.
     *
     * @param pointers      the pointers defining the state where to begin iteration
     * @param size          Count of how many entries will be fetched
     * @param entryConsumer the consumer to call with fetched key-value pairs
     * @return the pointers defining the state where iteration has ended
     */
    private IterationPointer[] fetchNext(IterationPointer[] pointers,
                                         int size,
                                         BiConsumer<K, V> entryConsumer) {
        long now = Clock.currentTimeMillis();
        Segment<K, V> segment = segments[0];
        try {
            segment.lock();
            HashEntry<K, V>[] currentTable = segment.table;
            int currentTableSize = currentTable.length;

            pointers = checkPointers(pointers, currentTableSize);
            IterationPointer lastPointer = pointers[pointers.length - 1];

            int nextTableIndex;
            if (lastPointer.getIndex() >= 0 && lastPointer.getIndex() < segment.table.length) {
                nextTableIndex = lastPointer.getIndex();
            } else {
                nextTableIndex = currentTable.length - 1;
            }
            int counter = 0;
            while (nextTableIndex >= 0 && counter < size) {
                HashEntry<K, V> nextEntry = currentTable[nextTableIndex--];
                while (nextEntry != null) {
                    V value = nextEntry.value();
                    if (isValidForFetching(value, now)) {
                        K key = nextEntry.key();
                        if (key != null && hasNotBeenObserved(key, pointers)) {
                            entryConsumer.accept(key, value);
                            counter++;
                        }
                    }
                    nextEntry = nextEntry.next;
                }
            }
            lastPointer.setIndex(nextTableIndex);
            return pointers;
        } finally {
            segment.unlock();
        }
    }

    /**
     * Checks the {@code pointers} to see if we need to restart iteration on the
     * current table and returns the updated pointers if necessary.
     *
     * @param pointers         the pointers defining the state of iteration
     * @param currentTableSize the current table size
     * @return the updated pointers, if necessary
     */
    private IterationPointer[] checkPointers(IterationPointer[] pointers, int currentTableSize) {
        IterationPointer lastPointer = pointers[pointers.length - 1];
        boolean iterationStarted = lastPointer.getSize() == -1;
        boolean tableResized = lastPointer.getSize() != currentTableSize;
        // clone pointers to avoid mutating given reference
        // add new pointer if resize happened during iteration
        int newLength = !iterationStarted && tableResized ? pointers.length + 1 : pointers.length;

        IterationPointer[] updatedPointers = new IterationPointer[newLength];
        for (int i = 0; i < pointers.length; i++) {
            updatedPointers[i] = new IterationPointer(pointers[i]);
        }

        // reset last pointer if we haven't started iteration or there was a resize
        if (iterationStarted || tableResized) {
            updatedPointers[updatedPointers.length - 1] = new IterationPointer(Integer.MAX_VALUE, currentTableSize);
        }
        return updatedPointers;
    }

    /**
     * Returns {@code true} if the given {@code key} has not been already observed
     * (or should have been observed) with the iteration state provided by the
     * {@code pointers}.
     *
     * @param key      the key to check
     * @param pointers the iteration state
     * @return if the key should have already been observed by the iteration user
     */
    private boolean hasNotBeenObserved(K key, IterationPointer[] pointers) {
        if (pointers.length < 2) {
            // there was no resize yet so we most definitely haven't observed the entry
            return true;
        }
        int hash = hashOf(key);
        // check only the pointers up to the last, we haven't observed it with the last pointer
        for (int i = 0; i < pointers.length - 1; i++) {
            IterationPointer iterationPointer = pointers[i];
            int tableSize = iterationPointer.getSize();
            int tableIndex = iterationPointer.getIndex();
            int index = hash & (tableSize - 1);
            if (index > tableIndex) {
                // entry would have been located after in the table on the given size
                // so we have observed it
                return false;
            }
        }
        return true;
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
