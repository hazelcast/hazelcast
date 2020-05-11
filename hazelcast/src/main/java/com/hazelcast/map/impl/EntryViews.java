/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.compatibility.map.CompatibilityWanMapEntryView;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.wan.WanMapEntryView;

/**
 * A class providing static factory methods that create various entry view objects.
 */
public final class EntryViews {

    private EntryViews() {
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView() {
        return new SimpleEntryView<>();
    }

    public static <K, V> EntryView<K, V> createSimpleEntryView(K key, V value, Record record) {
        return new SimpleEntryView<>(key, value)
                .withCost(record.getCost())
                .withVersion(record.getVersion())
                .withHits(record.getHits())
                .withLastAccessTime(record.getLastAccessTime())
                .withLastUpdateTime(record.getLastUpdateTime())
                .withTtl(record.getTtl())
                .withMaxIdle(record.getMaxIdle())
                .withCreationTime(record.getCreationTime())
                .withExpirationTime(record.getExpirationTime())
                .withLastStoredTime(record.getLastStoredTime());
    }

    public static <K, V> WanMapEntryView<K, V> createWanEntryView(Data key, Data value,
                                                                  Record<V> record,
                                                                  SerializationService serializationService) {
        return new WanMapEntryView<K, V>(key, value, serializationService)
                .withCost(record.getCost())
                .withVersion(record.getVersion())
                .withHits(record.getHits())
                .withLastAccessTime(record.getLastAccessTime())
                .withLastUpdateTime(record.getLastUpdateTime())
                .withTtl(record.getTtl())
                .withMaxIdle(record.getMaxIdle())
                .withCreationTime(record.getCreationTime())
                .withExpirationTime(record.getExpirationTime())
                .withLastStoredTime(record.getLastStoredTime());
    }

    public static <K, V> WanMapEntryView<K, V> createWanEntryView(Data key, Data value,
                                                                  CompatibilityWanMapEntryView compatibilityView,
                                                                  SerializationService serializationService) {
        return new WanMapEntryView<K, V>(key, value, serializationService)
                .withCost(compatibilityView.getCost())
                .withVersion(compatibilityView.getVersion())
                .withHits(compatibilityView.getHits())
                .withLastAccessTime(compatibilityView.getLastAccessTime())
                .withLastUpdateTime(compatibilityView.getLastUpdateTime())
                .withTtl(compatibilityView.getTtl())
                .withMaxIdle(compatibilityView.getMaxIdle())
                .withCreationTime(compatibilityView.getCreationTime())
                .withExpirationTime(compatibilityView.getExpirationTime())
                .withLastStoredTime(compatibilityView.getLastStoredTime());
    }
}
