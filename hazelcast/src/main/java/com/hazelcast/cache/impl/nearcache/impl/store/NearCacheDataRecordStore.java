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
    protected boolean isAvailable() {
        return store != null;
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        if (key instanceof Data) {
            return
                    // Reference to this key data inside map ("store" field)
                    REFERENCE_SIZE
                            // Heap cost of this key data
                            + ((Data) key).getHeapCost();
        } else {
            // Memory cost for non-data typed instance is not supported.
            return 0L;
        }
    }

    // TODO We don't handle object header (mark, class definition) for heap memory cost
    @Override
    protected long getRecordStorageMemoryCost(NearCacheDataRecord record) {
        Data value = record.getValue();
        return
                // Reference to this record inside map ("store" field)
                REFERENCE_SIZE
                        // Reference to "value" field
                        + REFERENCE_SIZE
                        // Heap cost of this value data
                        + (value != null ? value.getHeapCost() : 0)
                        // 3 primitive long typed fields: "creationTime", "expirationTime" and "accessTime"
                        + (3 * (Long.SIZE / Byte.SIZE))
                        // Reference to "accessHit" field
                        + REFERENCE_SIZE
                        // Primitive int typed "value" field in "AtomicInteger" typed "accessHit" field
                        + (Integer.SIZE / Byte.SIZE);
    }

    @Override
    protected NearCacheDataRecord valueToRecord(V value) {
        Data data = toData(value);
        long creationTime = Clock.currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return new NearCacheDataRecord(data, creationTime, creationTime + timeToLiveMillis);
        } else {
            return new NearCacheDataRecord(data, creationTime, NearCacheRecord.TIME_NOT_SET);
        }
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
    protected void putToRecord(NearCacheDataRecord record, V value) {
        record.setValue(toData(value));
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
