/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

/**
 * Provides static factory methods to create {@link MergingValue} and {@link MergingEntry} instances.
 *
 * @since 3.10
 */
public final class MergingValueFactory {

    private MergingValueFactory() {
    }

    public static <V> MergingValue<V> createMergingValue(SerializationService serializationService, V value) {
        return new MergingValueImpl<V>(serializationService)
                .setValue(value);
    }

    public static MergingValue<Data> createMergingValue(SerializationService serializationService, CollectionItem item) {
        return new MergingValueImpl<Data>(serializationService)
                .setValue(item.getValue());
    }

    public static MergingValue<Data> createMergingValue(SerializationService serializationService, QueueItem item) {
        return new MergingValueImpl<Data>(serializationService)
                .setValue(item.getData());
    }

    public static <K, V> MergingEntry<K, V> createMergingEntry(SerializationService serializationService, K key, V value) {
        return new MergingEntryImpl<K, V>(serializationService)
                .setKey(key)
                .setValue(value)
                .setCreationTime(Clock.currentTimeMillis());
    }

    public static <K, V> MergingEntry<K, V> createMergingEntry(SerializationService serializationService,
                                                               EntryView<K, V> entryView) {
        return new FullMergingEntryImpl<K, V>(serializationService)
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits())
                .setTtl(entryView.getTtl())
                .setVersion(entryView.getVersion())
                .setCost(entryView.getCost());
    }

    public static <K, V> MergingEntry<K, V> createMergingEntry(SerializationService serializationService,
                                                               CacheEntryView<K, V> entryView) {
        return new FullMergingEntryImpl<K, V>(serializationService)
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getAccessHit());
    }

    public static MergingEntry<Data, Data> createMergingEntry(SerializationService serializationService,
                                                              Record record, Data dataValue) {
        return new FullMergingEntryImpl<Data, Data>(serializationService)
                .setKey(record.getKey())
                .setValue(dataValue)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setVersion(record.getVersion())
                .setTtl(record.getTtl());
    }

    public static MergingEntry<Data, Object> createMergingEntry(SerializationService serializationService, Record record) {
        return new FullMergingEntryImpl<Data, Object>(serializationService)
                .setKey(record.getKey())
                .setValue(record.getValue())
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setVersion(record.getVersion())
                .setTtl(record.getTtl());
    }

    public static MergingEntry<Data, Object> createMergingEntry(SerializationService serializationService,
                                                                MultiMapMergeContainer container,
                                                                MultiMapRecord record) {
        return new FullMergingEntryImpl<Data, Object>(serializationService)
                .setKey(container.getKey())
                .setValue(record.getObject())
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(container.getHits());
    }

    public static MergingEntry<Data, Object> createMergingEntry(SerializationService serializationService,
                                                                MultiMapContainer container, Data key,
                                                                MultiMapRecord record, int hits) {
        return new FullMergingEntryImpl<Data, Object>(serializationService)
                .setKey(key)
                .setValue(record.getObject())
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(hits);
    }

    public static <R extends CacheRecord> MergingEntry<Data, Data> createMergingEntry(
            SerializationService serializationService, Data key, Data value, R record) {
        return new FullMergingEntryImpl<Data, Data>(serializationService)
                .setKey(key)
                .setValue(value)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getAccessHit())
                .setLastAccessTime(record.getLastAccessTime());
    }

    public static MergingEntry<Object, Object> createMergingEntry(SerializationService serializationService,
                                                                  ReplicatedRecord record) {
        return new FullMergingEntryImpl<Object, Object>(serializationService)
                .setKey(record.getKeyInternal())
                .setValue(record.getValueInternal())
                .setCreationTime(record.getCreationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getUpdateTime())
                .setTtl(record.getTtlMillis());
    }

    public static MergingEntry<String, HyperLogLog> createMergingEntry(SerializationService serializationService,
                                                                       String name, HyperLogLog item) {
        return new MergingEntryImpl<String, HyperLogLog>(serializationService)
                .setKey(name)
                .setValue(item)
                .setCreationTime(Clock.currentTimeMillis());
    }

    public static MergingEntry<String, ScheduledTaskDescriptor> createMergingEntry(SerializationService serializationService,
                                                                                   ScheduledTaskDescriptor task) {
        return new MergingEntryImpl<String, ScheduledTaskDescriptor>(serializationService)
                .setKey(task.getDefinition().getName())
                .setValue(task)
                .setCreationTime(Clock.currentTimeMillis());
    }
}
