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
import com.hazelcast.internal.nearcache.impl.record.NearCacheDataRecord;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import static com.hazelcast.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static com.hazelcast.internal.nearcache.impl.record.AbstractNearCacheRecord.NUMBER_OF_BOOLEAN_FIELD_TYPES;
import static com.hazelcast.internal.nearcache.impl.record.AbstractNearCacheRecord.NUMBER_OF_INTEGER_FIELD_TYPES;
import static com.hazelcast.internal.nearcache.impl.record.AbstractNearCacheRecord.NUMBER_OF_LONG_FIELD_TYPES;
import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * {@link com.hazelcast.internal.nearcache.NearCacheRecordStore} implementation for Near Caches
 * with {@link com.hazelcast.config.InMemoryFormat#BINARY} in-memory-format.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
public class NearCacheDataRecordStore<K, V> extends BaseHeapNearCacheRecordStore<K, V, NearCacheDataRecord> {

    public NearCacheDataRecordStore(String name,
                                    NearCacheConfig nearCacheConfig,
                                    SerializationService serializationService,
                                    ClassLoader classLoader) {
        super(name, nearCacheConfig, serializationService, classLoader);
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        if (key instanceof Data) {
            return
                    // reference to this key data inside map ("store" field)
                    REFERENCE_COST_IN_BYTES
                            // heap cost of this key data
                            + ((Data) key).getHeapCost();
        } else {
            // memory cost for non-data typed instance is not supported
            return 0L;
        }
    }

    @Override
    protected long getRecordStorageMemoryCost(NearCacheDataRecord record) {
        if (record == null) {
            return 0L;
        }
        // TODO: we don't handle object header (mark, class definition) for heap memory cost
        Data value = record.getValue();
        // reference to this record inside map ("store" field)
        return REFERENCE_COST_IN_BYTES
                // reference to "value" field
                + REFERENCE_COST_IN_BYTES
                // partition Id
                + (Integer.SIZE / Byte.SIZE)
                // "uuid" ref size + 2 long in uuid
                + REFERENCE_COST_IN_BYTES + (2 * (Long.SIZE / Byte.SIZE))
                // heap cost of this value data
                + (value != null ? value.getHeapCost() : 0)
                + NUMBER_OF_LONG_FIELD_TYPES * (Long.SIZE / Byte.SIZE)
                + NUMBER_OF_INTEGER_FIELD_TYPES * (Integer.SIZE / Byte.SIZE)
                + NUMBER_OF_BOOLEAN_FIELD_TYPES;
    }

    @Override
    protected NearCacheDataRecord createRecord(V value) {
        Data dataValue = toData(value);
        long creationTime = currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return new NearCacheDataRecord(dataValue, creationTime, creationTime + timeToLiveMillis);
        } else {
            return new NearCacheDataRecord(dataValue, creationTime, TIME_NOT_SET);
        }
    }

    @Override
    protected void updateRecordValue(NearCacheDataRecord record, V value) {
        record.setValue(toData(value));
    }
}
