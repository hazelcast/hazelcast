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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.impl.connector.AbstractUpdateMapP;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class WriteCdcP<K, V> extends AbstractUpdateMapP<ChangeRecord, K, V> {

    /**
     * This processor uses {@link IMap#submitToKeys(Set, EntryProcessor)}, which
     * if used from multiple parallel async operations can end up reordering
     * the changes done to the map and this in turn can result in unforseen
     * consequences. For this reason we need to limit ourselves to a single
     * in-flight operation at a time.
     */
    private static final int MAX_PARALLEL_ASYNC_OPS = 1;

    private static final int INITIAL_CAPACITY = 4 * 1024;
    private static final float LOAD_FACTOR = 0.75f;

    private final FunctionEx<? super ChangeRecord, ? extends V> valueFn;

    /**
     * Tracks the last seen sequence for a set of keys.
     * <p>
     * It behaves as a LRU cache, which means it evicts the entries that
     * haven't been accessed for the configured time (see the {@code
     * expirationMs} parameter). This limits memory consumption without
     * affecting the detection of reordering because the events are ordered at
     * the source and the Jet pipeline introduces a bounded amount of
     * reordering.
     */
    private LinkedHashMap<K, Sequence> sequences;

    public WriteCdcP(
            @Nonnull HazelcastInstance instance,
            @Nonnull String map,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends K> keyFn,
            @Nonnull FunctionEx<? super ChangeRecord, ? extends V> valueFn
    ) {
        super(instance, MAX_PARALLEL_ASYNC_OPS, map, keyFn);
        this.valueFn = valueFn;

    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        super.init(outbox, context);

        Properties properties = context.hazelcastInstance().getConfig().getProperties();
        HazelcastProperties hzProperties = new HazelcastProperties(properties);
        long expirationMs = hzProperties.getMillis(CdcSinks.SEQUENCE_CACHE_EXPIRATION_SECONDS);
        sequences = new LinkedHashMap<K, Sequence>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, Sequence> eldest) {
                return eldest.getValue().isOlderThan(expirationMs);
            }
        };
    }

    /**
     * @param key       key of an event that has just been observed
     * @param source    source of the event sequence number
     * @param sequence  numeric value of the event sequence number
     * @return true if the newly observed sequence number is more recent than any
     *         previously observed one
     */
    boolean updateSequence(K key, long source, long sequence) {
        Sequence prevSequence = sequences.get(key);
        if (prevSequence == null) { // first observed sequence for key
            sequences.put(key, new Sequence(source, sequence));
            return true;
        }
        return prevSequence.update(source, sequence);
    }

    @Override
    protected void addToBuffer(ChangeRecord item) {
        K key = keyFn.apply(item);
        if (shouldBeDropped(key, item)) {
            pendingItemCount--;
            return;
        }

        Data keyData = serializationContext.toKeyData(key);
        int partitionId = serializationContext.partitionId(keyData);

        Map<Data, Object> buffer = partitionBuffers[partitionId];
        Data value = serializationContext.toData(valueFn.apply(item));
        if (buffer.put(keyData, value) == null) {
            pendingInPartition[partitionId]++;
        } else { // item already exists, it will be coalesced
            pendingItemCount--;
        }
    }

    private boolean shouldBeDropped(K key, ChangeRecord item) {
        long sequenceSource = item.sequenceSource();
        long sequenceValue = item.sequenceValue();

        return !updateSequence(key, sequenceSource, sequenceValue);
    }

    @Override
    protected EntryProcessor<K, V, Void> entryProcessor(Map<Data, Object> buffer) {
        return new ApplyValuesEntryProcessor<>(buffer);
    }

    /**
     * Tracks a CDC event's sequence number and the moment when it was seen.
     * <p>
     * The timestamp is the system time taken when a sink instance sees an
     * event.
     * <p>
     * The <em>sequence</em> part is a monotonically increasing numeric
     * sequence which we base our ordering on.
     * <p>
     * The <em>source</em> part provides the scope of validity of the sequence
     * part. This is needed because many CDC sources don't provide a globally
     * valid sequence.
     */
    private static class Sequence {

        private long timestamp;
        private long source;
        private long sequence;

        Sequence(long source, long sequence) {
            this.timestamp = System.currentTimeMillis();
            this.source = source;
            this.sequence = sequence;
        }

        boolean isOlderThan(long ageLimitMs) {
            long age = System.currentTimeMillis() - timestamp;
            return age > ageLimitMs;
        }

        boolean update(long source, long sequence) {
            timestamp = System.currentTimeMillis();

            if (this.source != source) { //sequence source changed for key
                this.source = source;
                this.sequence = sequence;
                return true;
            }

            if (this.sequence < sequence) { //sequence is newer than previous for key
                this.sequence = sequence;
                return true;
            }

            return false;
        }
    }
}
