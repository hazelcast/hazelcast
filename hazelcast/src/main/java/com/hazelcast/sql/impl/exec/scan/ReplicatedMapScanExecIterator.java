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

import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;

import java.util.Collection;
import java.util.Iterator;

/**
 * Iterator over map partitions.
 */
@SuppressWarnings("rawtypes")
public class ReplicatedMapScanExecIterator implements KeyValueIterator {

    private final Iterator<ReplicatedRecordStore> recordStores;
    private Iterator<ReplicatedRecord> currentRecordStore;

    private Object currentKey;
    private Object currentValue;
    private Object nextKey;
    private Object nextValue;

    public ReplicatedMapScanExecIterator(ReplicatedMapProxy map) {
        ReplicatedMapService svc = (ReplicatedMapService) map.getService();

        Collection<ReplicatedRecordStore> stores = svc.getAllReplicatedRecordStores(map.getName());

        recordStores = stores.iterator();

        advance0();
    }

    @Override
    public boolean tryAdvance() {
        if (!done()) {
            currentKey = nextKey;
            currentValue = nextValue;

            advance0();

            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean done() {
        return nextKey == null;
    }

    /**
     * Get the next key/value pair from the store.
     */
    private void advance0() {
        while (true) {
            // Move to the next record store if needed.
            if (currentRecordStore == null) {
                if (!recordStores.hasNext()) {
                    nextKey = null;
                    nextValue = null;

                    return;
                } else {
                    ReplicatedRecordStore currentRecordStore = recordStores.next();

                    if (currentRecordStore == null) {
                        // RecordStore might be missing if the associated partition is empty. Just skip it.
                        continue;
                    }

                    this.currentRecordStore = currentRecordStore.recordIterator();
                }
            }

            assert currentRecordStore != null;

            // Move to the next valid entry inside the record store.
            if (currentRecordStore.hasNext()) {
                ReplicatedRecord entry = currentRecordStore.next();

                nextKey = entry.getKey();
                nextValue = entry.getValue();

                return;
            }

            // No matching entries in this store, move to the next one.
            currentRecordStore = null;
        }
    }

    @Override
    public Object getKey() {
        return currentKey;
    }

    @Override
    public Object getValue() {
        return currentValue;
    }
}
