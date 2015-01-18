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

package com.hazelcast.cache.impl.nearcache.impl.store;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.impl.record.NearCacheDataRecord;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NearCacheDataRecordStore<K, V>
        extends AbstractNearCacheRecordStore<K, V, NearCacheDataRecord> {

    private ConcurrentMap<K, NearCacheDataRecord> store =
            new ConcurrentHashMap<K, NearCacheDataRecord>();

    public NearCacheDataRecordStore(NearCacheConfig nearCacheConfig, NearCacheContext nearCacheContext) {
        super(nearCacheConfig, nearCacheContext);
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        // TODO Calculate size of key data and consider also its reference size
        // since it is pointed from internal map
        return 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(NearCacheDataRecord record) {
        // TODO Calculate size of record (with its attributes and references) and
        // also consider its reference size since it is pointed from internal map
        return 0L;
    }

    @Override
    protected NearCacheDataRecord valueToRecord(V value) {
        Data data = valueToData(value);
        long creationTime = Clock.currentTimeMillis();
        return new NearCacheDataRecord(data, creationTime, creationTime + timeToLiveMillis);
    }

    @Override
    protected V recordToValue(NearCacheDataRecord record) {
        Data data = record.getValue();
        return dataToValue(data);
    }

    @Override
    protected NearCacheDataRecord getRecord(K key) {
        return store.get(key);
    }

    @Override
    protected NearCacheDataRecord putRecord(K key, NearCacheDataRecord record) {
        return store.put(key, record);
    }

    @Override
    protected NearCacheDataRecord removeRecord(K key) {
        return store.remove(key);
    }

    @Override
    protected void clearRecords() {
        store.clear();
    }

    @Override
    protected void destroyStore() {
        clearRecords();
        // Clear reference so GC can collect it
        store = null;
    }

    @Override
    public Object selectToSave(Object... candidates) {
        Object selectedCandidate = null;
        if (candidates != null && candidates.length > 0) {
            for (Object candidate : candidates) {
                // Give priority to Data typed candidate.
                // So there will be no extra convertion from Object to Data.
                if (candidate instanceof Data) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                return candidates[0];
            }
        }
        return selectedCandidate;
    }

}
