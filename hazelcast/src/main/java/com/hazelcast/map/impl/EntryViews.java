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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * A class providing static factory methods that create various entry view objects.
 */
public final class EntryViews {

    private EntryViews() {
    }

    /**
     * Creates a null entry view that has only key and no value.
     *
     * @param key the key object which will be wrapped in {@link com.hazelcast.core.EntryView}.
     * @param <K> the type of key.
     * @param <V> the type of value.
     * @return returns  null entry view.
     */
    public static <K, V> EntryView<K, V> createNullEntryView(K key) {
        return new NullEntryView<K, V>(key);
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView() {
        return new SimpleEntryView<K, V>();
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView(K key, V value, Record record) {
        return new SimpleEntryView<K, V>(key, value)
                .withCost(record.getCost())
                .withVersion(record.getVersion())
                .withHits(record.getHits())
                .withLastAccessTime(record.getLastAccessTime())
                .withLastUpdateTime(record.getLastUpdateTime())
                .withTtl(record.getTtl())
                .withCreationTime(record.getCreationTime())
                .withExpirationTime(record.getExpirationTime())
                .withLastStoredTime(record.getLastStoredTime());
    }

    public static EntryView<Data, Data> toSimpleEntryView(Record<Data> record) {
        return new SimpleEntryView<Data, Data>(record.getKey(), record.getValue())
                .withCost(record.getCost())
                .withVersion(record.getVersion())
                .withHits(record.getHits())
                .withLastAccessTime(record.getLastAccessTime())
                .withLastUpdateTime(record.getLastUpdateTime())
                .withTtl(record.getTtl())
                .withCreationTime(record.getCreationTime())
                .withExpirationTime(record.getExpirationTime())
                .withLastStoredTime(record.getLastStoredTime());
    }

    public static <K, V> EntryView<K, V> createLazyEntryView(K key, V value, Record record,
                                                             SerializationService serializationService,
                                                             MapMergePolicy mergePolicy) {
        return new LazyEntryView<K, V>(key, value, serializationService, mergePolicy)
                .setCost(record.getCost())
                .setVersion(record.getVersion())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setTtl(record.getTtl())
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setLastStoredTime(record.getLastStoredTime());
    }

    public static <K, V> EntryView<K, V> convertToLazyEntryView(EntryView<K, V> entryView,
                                                                SerializationService serializationService,
                                                                MapMergePolicy mergePolicy) {
        return new LazyEntryView<K, V>(entryView.getKey(), entryView.getValue(), serializationService, mergePolicy)
                .setCost(entryView.getCost())
                .setVersion(entryView.getVersion())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setTtl(entryView.getTtl())
                .setCreationTime(entryView.getCreationTime())
                .setHits(entryView.getHits())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastStoredTime(entryView.getLastStoredTime());
    }
}
