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

package com.hazelcast.spi.merge;

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
import com.hazelcast.util.Clock;

/**
 * Provides static factory methods to create {@link MergeDataHolder} instances.
 *
 * @since 3.10
 */
public final class MergeDataHolders {

    private MergeDataHolders() {
    }

    public static <V> MergeDataHolder<V> createSplitBrainMergeEntryView(V value) {
        return new SimpleMergeDataHolder<Object, V>()
                .setValue(value)
                .setCreationTime(Clock.currentTimeMillis());
    }

    public static <K, V> KeyMergeDataHolder<K, V> createSplitBrainMergeEntryView(K key, V value) {
        return new SimpleMergeDataHolder<K, V>()
                .setKey(key)
                .setValue(value)
                .setCreationTime(Clock.currentTimeMillis());
    }

    public static <K, V> KeyMergeDataHolder<K, V> createSplitBrainMergeEntryView(EntryView<K, V> entryView) {
        return new SimpleMergeDataHolder<K, V>()
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits())
                .setTtl(entryView.getTtl())
                .setVersion(entryView.getVersion())
                .setCost(entryView.getCost());
    }

    public static <K, V> MergeDataHolder<V> createSplitBrainMergeEntryView(CacheEntryView<K, V> entryView) {
        return new SimpleMergeDataHolder<K, V>()
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getAccessHit());
    }

    public static MergeDataHolder<Data> createSplitBrainMergeEntryView(CollectionItem item) {
        return new SimpleMergeDataHolder<Long, Data>()
                .setKey(item.getItemId())
                .setValue(item.getValue())
                .setCreationTime(item.getCreationTime());
    }

    public static MergeDataHolder<Data> createSplitBrainMergeEntryView(QueueItem item) {
        return new SimpleMergeDataHolder<Long, Data>()
                .setKey(item.getItemId())
                .setValue(item.getData())
                .setCreationTime(item.getCreationTime());
    }

    public static KeyMergeDataHolder<Data, Object> createSplitBrainMergeEntryView(MultiMapMergeContainer container,
                                                                                  MultiMapRecord record) {
        return new SimpleMergeDataHolder<Data, Object>()
                .setKey(container.getKey())
                .setValue(record.getObject())
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(container.getHits());
    }

    public static KeyMergeDataHolder<Data, Object> createSplitBrainMergeEntryView(MultiMapContainer container, Data key,
                                                                                  MultiMapRecord record, int hits) {
        return new SimpleMergeDataHolder<Data, Object>()
                .setKey(key)
                .setValue(record.getObject())
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(hits);
    }

    public static KeyMergeDataHolder<Data, Data> createSplitBrainMergeEntryView(Record record, Data dataValue) {
        return new SimpleMergeDataHolder<Data, Data>()
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

    public static KeyMergeDataHolder<Data, Object> createSplitBrainMergeEntryView(Record record) {
        return new SimpleMergeDataHolder<Data, Object>()
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

    public static <R extends CacheRecord> KeyMergeDataHolder<Data, Data> createSplitBrainMergeEntryView(Data key, Data value,
                                                                                                        R record) {
        return new SimpleMergeDataHolder<Data, Data>()
                .setKey(key)
                .setValue(value)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getAccessHit())
                .setLastAccessTime(record.getLastAccessTime());
    }

    public static KeyMergeDataHolder<Object, Object> createSplitBrainMergeEntryView(ReplicatedRecord record) {
        return new SimpleMergeDataHolder<Object, Object>()
                .setKey(record.getKeyInternal())
                .setValue(record.getValueInternal())
                .setCreationTime(record.getCreationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getUpdateTime())
                .setTtl(record.getTtlMillis());
    }

    public static KeyMergeDataHolder<String, HyperLogLog> createSplitBrainMergeEntryView(String name, HyperLogLog item) {
        return new SimpleMergeDataHolder<String, HyperLogLog>()
                .setKey(name)
                .setValue(item);
    }

    public static KeyMergeDataHolder<String, ScheduledTaskDescriptor> createSplitBrainMergeEntryView(
            ScheduledTaskDescriptor task) {
        return new SimpleMergeDataHolder<String, ScheduledTaskDescriptor>()
                .setKey(task.getDefinition().getName())
                .setValue(task);
    }
}
