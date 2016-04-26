/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.record.RecordStatistics;
import com.hazelcast.map.merge.MapMergePolicy;
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

    public static <K, V> EntryView<K, V> createSimpleEntryView(K key, V value, Record record) {
        final SimpleEntryView simpleEntryView = new SimpleEntryView(key, value);
        simpleEntryView.setCost(record.getCost());
        simpleEntryView.setVersion(record.getVersion());
        simpleEntryView.setHits(record.getHits());
        simpleEntryView.setLastAccessTime(record.getLastAccessTime());
        simpleEntryView.setLastUpdateTime(record.getLastUpdateTime());
        simpleEntryView.setTtl(record.getTtl());
        simpleEntryView.setCreationTime(record.getCreationTime());

        final RecordStatistics statistics = record.getStatistics();
        if (statistics != null) {
            simpleEntryView.setExpirationTime(statistics.getExpirationTime());
            simpleEntryView.setLastStoredTime(statistics.getLastStoredTime());
        }
        return simpleEntryView;
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView() {
        return new SimpleEntryView<K, V>();
    }

    public static <K, V> EntryView<K, V> createLazyEntryView(K key, V value, Record record
            , SerializationService serializationService, MapMergePolicy mergePolicy) {
        final LazyEntryView lazyEntryView = new LazyEntryView(key, value, serializationService, mergePolicy);
        lazyEntryView.setCost(record.getCost());
        lazyEntryView.setVersion(record.getVersion());
        lazyEntryView.setHits(record.getHits());
        lazyEntryView.setLastAccessTime(record.getLastAccessTime());
        lazyEntryView.setLastUpdateTime(record.getLastUpdateTime());
        lazyEntryView.setTtl(record.getTtl());
        lazyEntryView.setCreationTime(record.getCreationTime());
        final RecordStatistics statistics = record.getStatistics();
        if (statistics != null) {
            lazyEntryView.setExpirationTime(statistics.getExpirationTime());
            lazyEntryView.setLastStoredTime(statistics.getLastStoredTime());
        }
        return lazyEntryView;
    }

    public static <K, V> EntryView<K, V> convertToLazyEntryView(EntryView entryView,
                                                                SerializationService serializationService,
                                                                MapMergePolicy mergePolicy) {
        final LazyEntryView lazyEntryView = new LazyEntryView(entryView.getKey(), entryView.getValue(),
                serializationService, mergePolicy);
        lazyEntryView.setCost(entryView.getCost());
        lazyEntryView.setVersion(entryView.getVersion());
        lazyEntryView.setLastAccessTime(entryView.getLastAccessTime());
        lazyEntryView.setLastUpdateTime(entryView.getLastUpdateTime());
        lazyEntryView.setTtl(entryView.getTtl());
        lazyEntryView.setCreationTime(entryView.getCreationTime());
        lazyEntryView.setHits(entryView.getHits());
        lazyEntryView.setExpirationTime(entryView.getExpirationTime());
        lazyEntryView.setLastStoredTime(entryView.getLastStoredTime());
        return lazyEntryView;
    }

}
