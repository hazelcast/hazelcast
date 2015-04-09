/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.cache.impl.nearcache.impl.record.NearCacheObjectRecord;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NearCacheObjectRecordStore<K, V>
        extends AbstractNearCacheRecordStore<K, V, NearCacheObjectRecord> {

    private ConcurrentMap<K, NearCacheObjectRecord> store =
            new ConcurrentHashMap<K, NearCacheObjectRecord>();

    public NearCacheObjectRecordStore(NearCacheConfig nearCacheConfig, NearCacheContext nearCacheContext) {
        super(nearCacheConfig, nearCacheContext);
    }

    @Override
    protected boolean isAvailable() {
        return store != null;
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        // Memory cost for "OBJECT" in memory format is totally not supported.
        // So just return zero.
        return 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(NearCacheObjectRecord record) {
        // Memory cost for "OBJECT" in memory format is totally not supported.
        // So just return zero.
        return 0L;
    }

    @Override
    protected NearCacheObjectRecord valueToRecord(V value) {
        value = toValue(value);
        long creationTime = Clock.currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return new NearCacheObjectRecord(value, creationTime, creationTime + timeToLiveMillis);
        } else {
            return new NearCacheObjectRecord(value, creationTime, NearCacheRecord.TIME_NOT_SET);
        }
    }

    @Override
    protected V recordToValue(NearCacheObjectRecord record) {
        return (V) record.getValue();
    }

    @Override
    protected NearCacheObjectRecord getRecord(K key) {
        return store.get(key);
    }

    @Override
    protected NearCacheObjectRecord putRecord(K key, NearCacheObjectRecord record) {
        return store.put(key, record);
    }

    @Override
    protected void putToRecord(NearCacheObjectRecord record, V value) {
        record.setValue(value);
    }

    @Override
    protected NearCacheObjectRecord removeRecord(K key) {
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
                // Give priority to non Data typed candidate.
                // So there will be no extra convertion from Data to Object.
                if (!(candidate instanceof Data)) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                // Select a non-null candidate
                for (Object candidate : candidates) {
                    if (candidate != null) {
                        selectedCandidate = candidate;
                        break;
                    }
                }
            }
        }
        return selectedCandidate;
    }

    @Override
    public int size() {
        checkAvailable();

        return store.size();
    }

}
