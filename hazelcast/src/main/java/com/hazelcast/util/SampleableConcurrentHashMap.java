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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

/**
 * ConcurrentHashMap to extend iterator capability
 *
 * @param <K> Type of the key
 * @param <V> Type of the value
 *
 * @author sozal 26/11/14
 */
public class SampleableConcurrentHashMap<K, V> extends ConcurrentReferenceHashMap<K, V> {

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
     * Entry to define keys and values for sampling.
     */
    public class SamplingEntry extends SimpleEntry<K, V> {

        public SamplingEntry(K key, V value) {
            super(key, value);
        }

        public final V setValue(V value) {
            throw new UnsupportedOperationException("Setting value is not supported in SamplingEntry");
        }

    }

    protected <E extends SamplingEntry> E createSamplingEntry(K key, V value) {
        return (E) new SamplingEntry(key, value);
    }

    //CHECKSTYLE:OFF
    /**
     * Gets and returns samples as <code>sampleCount</code>.
     *
     * NOTE: Originally taken by Jaromir Hamala's implementation. So kudos to Jaromir :)
     *
     * @param sampleCount Count of samples
     *
     * @return the sampled {@link SamplingEntry} list
     */
    public <E extends SamplingEntry> List<E> getRandomSamples(int sampleCount) {
        // TODO Samples can be returned as lazy so there will be no need to any collection
        // such as List, Set, etc ... to hold sampling entries

        if (sampleCount < 0) {
            throw new IllegalArgumentException("Sample count cannot be a negative value.");
        }
        if (sampleCount == 0) {
            return Collections.EMPTY_LIST;
        }

        final List<E> samples = new ArrayList<E>(sampleCount);
        final int randomNumber = Math.abs(THREAD_LOCAL_RANDOM.get().nextInt());
        final int firstSegmentIndex = randomNumber % segments.length;
        int currentSegmentIndex = firstSegmentIndex;

        do {
            Segment<K, V> segment = segments[currentSegmentIndex];
            if (segment != null) {
                HashEntry<K, V>[] table = segment.table;
                int firstBucketIndex = randomNumber % table.length;
                int currentBucketIndex = firstBucketIndex;
                do {
                    for (HashEntry<K, V> entry = table[currentBucketIndex]; entry != null; entry = entry.next) {
                        V value = entry.value();
                        if (value != null) {
                            samples.add((E) createSamplingEntry(entry.key(), value));
                            if (samples.size() == sampleCount) {
                                return samples;
                            }
                        }
                    }
                    currentBucketIndex = ++currentBucketIndex < table.length ? currentBucketIndex : 0;
                } while (currentBucketIndex != firstBucketIndex);
            }
            currentSegmentIndex = ++currentSegmentIndex < segments.length ? currentSegmentIndex : 0;
        } while (currentSegmentIndex != firstSegmentIndex);
        return samples;
    }
    //CHECKSTYLE:ON

}
