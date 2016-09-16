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

package com.hazelcast.cache.impl.nearcache.impl.store;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.cache.impl.nearcache.impl.record.NearCacheDataRecord;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;

public class NearCacheDataRecordStore<K, V>
        extends BaseHeapNearCacheRecordStore<K, V, NearCacheDataRecord> {

    public NearCacheDataRecordStore(NearCacheConfig nearCacheConfig, NearCacheContext nearCacheContext) {
        super(nearCacheConfig, nearCacheContext);
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        if (key instanceof Data) {
            return
                    // reference to this key data inside map ("store" field)
                    REFERENCE_SIZE
                            // heap cost of this key data
                            + ((Data) key).getHeapCost();
        } else {
            // memory cost for non-data typed instance is not supported
            return 0L;
        }
    }

    // TODO: we don't handle object header (mark, class definition) for heap memory cost
    @Override
    protected long getRecordStorageMemoryCost(NearCacheDataRecord record) {
        if (record == null) {
            return 0L;
        }
        Data value = record.getValue();
        return
                // reference to this record inside map ("store" field)
                REFERENCE_SIZE
                        // reference to "value" field
                        + REFERENCE_SIZE
                        // heap cost of this value data
                        + (value != null ? value.getHeapCost() : 0)
                        // 3 primitive long typed fields: "creationTime", "expirationTime" and "accessTime"
                        + (3 * (Long.SIZE / Byte.SIZE))
                        // reference to "accessHit" field
                        + REFERENCE_SIZE
                        // primitive int typed "value" field in "AtomicInteger" typed "accessHit" field
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
        if (record.getValue() == null) {
            nearCacheStats.incrementMisses();
            return (V) NULL_OBJECT;
        }
        Data data = record.getValue();
        return dataToValue(data);
    }

    @Override
    protected void putToRecord(NearCacheDataRecord record, V value) {
        record.setValue(toData(value));
    }

    @Override
    public Object selectToSave(Object... candidates) {
        Object selectedCandidate = null;
        if (candidates != null && candidates.length > 0) {
            for (Object candidate : candidates) {
                // give priority to Data typed candidate, so there will be no extra conversion from Object to Data
                if (candidate instanceof Data) {
                    selectedCandidate = candidate;
                    break;
                }
            }
            if (selectedCandidate != null) {
                return selectedCandidate;
            } else {
                // select a non-null candidate
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
}
