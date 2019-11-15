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

package com.hazelcast.spi.impl.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapMergeContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.ringbuffer.impl.Ringbuffer;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicLongMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicReferenceMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CardinalityEstimatorMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CollectionMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.QueueMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ReplicatedMapMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ScheduledExecutorMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;

/**
 * Provides static factory methods to create {@link MergingValue} and {@link MergingEntry} instances.
 *
 * @since 3.10
 */
public final class MergingValueFactory {

    private MergingValueFactory() {
    }

    public static CollectionMergeTypes createMergingValue(SerializationService serializationService,
                                                          Collection<CollectionItem> items) {
        Collection<Object> values = new ArrayList<Object>(items.size());
        for (CollectionItem item : items) {
            values.add(item.getValue());
        }
        return new CollectionMergingValueImpl(serializationService)
                .setValue(values);
    }

    public static QueueMergeTypes createMergingValue(SerializationService serializationService, Queue<QueueItem> items) {
        Collection<Object> values = new ArrayList<Object>(items.size());
        for (QueueItem item : items) {
            values.add(item.getData());
        }
        return new QueueMergingValueImpl(serializationService)
                .setValue(values);
    }

    public static AtomicLongMergeTypes createMergingValue(SerializationService serializationService, Long value) {
        return new AtomicLongMergingValueImpl(serializationService)
                .setValue(value);
    }

    public static AtomicReferenceMergeTypes createMergingValue(SerializationService serializationService, Data value) {
        return new AtomicReferenceMergingValueImpl(serializationService)
                .setValue(value);
    }

    public static MapMergeTypes createMergingEntry(SerializationService serializationService, EntryView<Data, Data> entryView) {
        return new MapMergingEntryImpl(serializationService)
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastStoredTime(entryView.getLastStoredTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits())
                .setTtl(entryView.getTtl())
                .setMaxIdle(entryView.getMaxIdle())
                .setVersion(entryView.getVersion())
                .setCost(entryView.getCost());
    }

    public static MapMergeTypes createMergingEntry(SerializationService serializationService, Record record) {
        return new MapMergingEntryImpl(serializationService)
                .setKey(record.getKey())
                .setValue(serializationService.toData(record.getValue()))
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setHits(record.getHits())
                .setTtl(record.getTtl())
                .setMaxIdle(record.getMaxIdle())
                .setVersion(record.getVersion())
                .setCost(record.getCost());
    }

    public static MapMergeTypes createMergingEntry(SerializationService serializationService,
                                                   Data dataKey, Data dataValue, Record record) {
        return new MapMergingEntryImpl(serializationService)
                .setKey(dataKey)
                .setValue(dataValue)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setHits(record.getHits())
                .setTtl(record.getTtl())
                .setMaxIdle(record.getMaxIdle())
                .setVersion(record.getVersion())
                .setCost(record.getCost());
    }

    public static CacheMergeTypes createMergingEntry(SerializationService serializationService,
                                                     CacheEntryView<Data, Data> entryView) {
        return new CacheMergingEntryImpl(serializationService)
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits());
    }

    public static <R extends CacheRecord> CacheMergeTypes createMergingEntry(SerializationService serializationService,
                                                                             Data key, Data value, R record) {
        return new CacheMergingEntryImpl(serializationService)
                .setKey(key)
                .setValue(value)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setLastAccessTime(record.getLastAccessTime())
                .setHits(record.getHits());
    }

    public static ReplicatedMapMergeTypes createMergingEntry(SerializationService serializationService, ReplicatedRecord record) {
        return new ReplicatedMapMergingEntryImpl(serializationService)
                .setKey(record.getKeyInternal())
                .setValue(record.getValueInternal())
                .setCreationTime(record.getCreationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getUpdateTime())
                .setTtl(record.getTtlMillis());
    }

    public static MultiMapMergeTypes createMergingEntry(SerializationService serializationService,
                                                        MultiMapMergeContainer container) {
        Collection<Object> values = new ArrayList<Object>(container.getRecords().size());
        for (MultiMapRecord record : container.getRecords()) {
            values.add(record.getObject());
        }

        return new MultiMapMergingEntryImpl(serializationService)
                .setKey(container.getKey())
                .setValues(values)
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(container.getHits());
    }

    public static MultiMapMergeTypes createMergingEntry(SerializationService serializationService, MultiMapContainer container,
                                                        Data dataKey, Collection<MultiMapRecord> records, long hits) {
        Collection<Object> values = new ArrayList<Object>(records.size());
        for (MultiMapRecord record : records) {
            values.add(record.getObject());
        }

        return new MultiMapMergingEntryImpl(serializationService)
                .setKey(dataKey)
                .setValues(values)
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(hits);
    }

    public static RingbufferMergeTypes createMergingValue(SerializationService serializationService, Ringbuffer<Object> items) {
        RingbufferMergeData mergingData = new RingbufferMergeData(items);
        return new RingbufferMergingValueImpl(serializationService)
                .setValues(mergingData);
    }

    public static CardinalityEstimatorMergeTypes createMergingEntry(SerializationService serializationService,
                                                                    String name, HyperLogLog hyperLogLog) {
        return new CardinalityEstimatorMergingEntry(serializationService)
                .setKey(name)
                .setValue(hyperLogLog);
    }

    public static ScheduledExecutorMergeTypes createMergingEntry(SerializationService serializationService,
                                                                 ScheduledTaskDescriptor task) {
        return new ScheduledExecutorMergingEntryImpl(serializationService)
                .setKey(task.getDefinition().getName())
                .setValue(task);
    }
}
