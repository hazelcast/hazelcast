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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;

import java.util.Iterator;
import java.util.Map;

/**
 * Iterator over map partitions.
 */
@SuppressWarnings("rawtypes")
public class MapScanExecIterator {

    private final MapContainer map;
    private final Iterator<Integer> partsIterator;
    private final long now = Clock.currentTimeMillis();

    private RecordStore currentRecordStore;
    private Iterator<Map.Entry<Data, Record<Object>>> currentRecordStoreIterator;

    private Data currentKey;
    private Object currentValue;
    private Data nextKey;
    private Object nextValue;

    public MapScanExecIterator(MapContainer map, Iterator<Integer> partsIterator) {
        this.map = map;
        this.partsIterator = partsIterator;

        advance0();
    }

    public boolean tryAdvance() {
        if (hasNext()) {
            currentKey = nextKey;
            currentValue = nextValue;

            advance0();

            return true;
        } else {
            return false;
        }
    }

    public boolean hasNext() {
        return nextKey != null;
    }

    /**
     * Get the next key/value pair from the store.
     */
    @SuppressWarnings("unchecked")
    private void advance0() {
        while (true) {
            // Move to the next record store if needed.
            if (currentRecordStoreIterator == null) {
                if (!partsIterator.hasNext()) {
                    nextKey = null;
                    nextValue = null;

                    return;
                } else {
                    int nextPart = partsIterator.next();

                    boolean isOwned = map.getMapServiceContext().getOwnedPartitions().contains(nextPart);

                    if (!isOwned) {
                        throw QueryException.error(SqlErrorCode.PARTITION_MIGRATED,
                            "Partition is not owned by member: " + nextPart);
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

                if (!currentRecordStore.isExpired(entry.getValue(), now, false)) {
                    nextKey = entry.getKey();
                    nextValue = entry.getValue().getValue();

                    return;
                }
            }

            // No matching entries in this store, move to the next one.
            currentRecordStore = null;
            currentRecordStoreIterator = null;
        }
    }

    public Object getKey() {
        return currentKey;
    }

    public Object getValue() {
        return currentValue;
    }
}
