/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;

import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.map.impl.record.Records.getValueOrCachedValue;

/**
 * Iterator over map partitions.
 */
@SuppressWarnings("rawtypes")
public class MapScanExecIterator implements KeyValueIterator {

    private final MapContainer map;
    private final Iterator<Integer> partsIterator;
    private final InternalSerializationService serializationService;
    private final long now = Clock.currentTimeMillis();

    private RecordStore currentRecordStore;
    private Iterator<Map.Entry<Data, Record<Object>>> currentRecordStoreIterator;

    private Data currentKeyData;
    private Object currentValue;
    private Data currentValueData;
    private Data nextKeyData;
    private Object nextValue;
    private Data nextValueData;

    private boolean useCachedValues;

    public MapScanExecIterator(
        MapContainer map,
        Iterator<Integer> partsIterator,
        InternalSerializationService serializationService
    ) {
        this.map = map;
        this.partsIterator = partsIterator;
        this.serializationService = serializationService;

        advance0(true);
    }

    @Override
    public boolean tryAdvance() {
        if (!done()) {
            currentKeyData = nextKeyData;
            currentValue = nextValue;
            currentValueData = nextValueData;

            advance0(false);

            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean done() {
        return nextKeyData == null;
    }

    /**
     * Get the next key/value pair from the store.
     */
    @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity"})
    private void advance0(boolean first) {
        while (true) {
            // Move to the next record store if needed.
            if (currentRecordStoreIterator == null) {
                if (!partsIterator.hasNext()) {
                    nextKeyData = null;
                    nextValue = null;
                    nextValueData = null;

                    return;
                } else {
                    int nextPart = partsIterator.next();

                    boolean isOwned = map.getMapServiceContext().getOrInitCachedMemberPartitions().contains(nextPart);

                    if (!isOwned) {
                        throw QueryException.error(
                                SqlErrorCode.PARTITION_DISTRIBUTION,
                                "Partition was migrated while a query was executed: "
                                        + "Partition is not owned by member: " + nextPart
                        ).markInvalidate();
                    }

                    if (first) {
                        useCachedValues = map.isUseCachedDeserializedValuesEnabled(nextPart);
                    }

                    currentRecordStore = map.getMapServiceContext().getRecordStore(nextPart, map.getName());

                    if (currentRecordStore == null) {
                        // RecordStore might be missing if the associated partition is empty. Just skip it.
                        continue;
                    }

                    try {
                        currentRecordStore.checkIfLoaded();
                    } catch (RetryableHazelcastException e) {
                        throw QueryException.error(SqlErrorCode.MAP_LOADING_IN_PROGRESS, "Map loading is in progress: "
                                + map.getName(), e);
                    }

                    currentRecordStoreIterator = currentRecordStore.getStorage().mutationTolerantIterator();
                }
            }

            assert currentRecordStoreIterator != null;

            // Move to the next valid entry inside the record store.
            while (currentRecordStoreIterator.hasNext()) {
                Map.Entry<Data, Record<Object>> entry = currentRecordStoreIterator.next();

                if (!currentRecordStore.isExpired(entry.getKey(), now, false)) {
                    nextKeyData = entry.getKey();

                    Record record = entry.getValue();

                    nextValue = useCachedValues ? getValueOrCachedValue(record, serializationService) : record.getValue();

                    if (nextValue instanceof Data) {
                        nextValueData = (Data) nextValue;
                        nextValue = null;
                    } else {
                        Object possiblyData = record.getValue();

                        nextValueData = possiblyData instanceof Data ? (Data) possiblyData : null;
                    }

                    return;
                }
            }

            // No matching entries in this store, move to the next one.
            currentRecordStore = null;
            currentRecordStoreIterator = null;
        }
    }

    @Override
    public Object getKey() {
        return null;
    }

    @Override
    public Data getKeyData() {
        return currentKeyData;
    }

    @Override
    public Object getValue() {
        return currentValue;
    }

    @Override
    public Data getValueData() {
        return currentValueData;
    }
}
