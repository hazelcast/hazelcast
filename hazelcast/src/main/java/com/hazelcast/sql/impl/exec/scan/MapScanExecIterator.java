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
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class MapScanExecIterator {
    private final MapProxyImpl map;
    private final Iterator<Integer> partsIterator;
    private final long now = Clock.currentTimeMillis();
    private RecordStore currentRecordStore;
    private Iterator<Map.Entry<Data, Record<Object>>> currentRecordStoreIterator;
    private Map.Entry<Data, Record<Object>> current;
    private Map.Entry<Data, Record<Object>> next;

    public MapScanExecIterator(MapProxyImpl map, PartitionIdSet parts) {
        this.map = map;

        partsIterator = parts.iterator();

        next = advance0();
    }

    public boolean tryAdvance() {
        if (next != null) {
            current = next;

            next = advance0();

            return true;
        } else {
            return false;
        }
    }

    public boolean canAdvance() {
        return next != null;
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Data, Record<Object>> advance0() {
        while (true) {
            // Move to the next record store if needed.
            if (currentRecordStoreIterator == null) {
                if (!partsIterator.hasNext()) {
                    return null;
                } else {
                    int nextPart = partsIterator.next();

                    currentRecordStore =
                        map.getMapServiceContext().getPartitionContainer(nextPart).getRecordStore(map.getName());

                    currentRecordStore.checkIfLoaded();

                    currentRecordStoreIterator = currentRecordStore.iterator();
                }
            }

            assert currentRecordStoreIterator != null;

            // Move to the next valid entry.
            while (currentRecordStoreIterator.hasNext()) {
                Map.Entry<Data, Record<Object>> entry = currentRecordStoreIterator.next();

                if (!currentRecordStore.isExpired(entry.getValue(), now, false)) {
                    return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue());
                }
            }

            // No matching entries in this store, move to the next one.
            currentRecordStore = null;
            currentRecordStoreIterator = null;
        }
    }

    public Object getKey() {
        return current.getKey();
    }

    public Object getValue() {
        return current.getValue().getValue();
    }
}
