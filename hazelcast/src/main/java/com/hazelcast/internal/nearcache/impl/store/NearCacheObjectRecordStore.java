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

package com.hazelcast.internal.nearcache.impl.store;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.impl.record.NearCacheObjectRecord;
import com.hazelcast.internal.serialization.SerializationService;

import static com.hazelcast.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static com.hazelcast.internal.util.Clock.currentTimeMillis;

/**
 * {@link com.hazelcast.internal.nearcache.NearCacheRecordStore} implementation for Near Caches
 * with {@link com.hazelcast.config.InMemoryFormat#OBJECT} in-memory-format.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
public class NearCacheObjectRecordStore<K, V> extends BaseHeapNearCacheRecordStore<K, V, NearCacheObjectRecord<V>> {

    public NearCacheObjectRecordStore(String name,
                                      NearCacheConfig nearCacheConfig,
                                      SerializationService serializationService,
                                      ClassLoader classLoader) {
        super(name, nearCacheConfig, serializationService, classLoader);
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        // memory cost for "OBJECT" in memory format is totally not supported, so just return zero
        return 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(NearCacheObjectRecord record) {
        // memory cost for "OBJECT" in memory format is totally not supported, so just return zero
        return 0L;
    }

    @Override
    protected NearCacheObjectRecord<V> createRecord(V value) {
        value = toValue(value);
        long creationTime = currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return new NearCacheObjectRecord<V>(value, creationTime, creationTime + timeToLiveMillis);
        } else {
            return new NearCacheObjectRecord<V>(value, creationTime, TIME_NOT_SET);
        }
    }

    @Override
    protected void updateRecordValue(NearCacheObjectRecord<V> record, V value) {
        record.setValue(toValue(value));
    }
}
