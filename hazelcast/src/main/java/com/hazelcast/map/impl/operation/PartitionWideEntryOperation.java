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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * GOTCHA : This operation does NOT load missing keys from map-store for now.
 */
public class PartitionWideEntryOperation extends AbstractMultipleEntryOperation implements BackupAwareOperation {

    public PartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name, entryProcessor);
    }

    public PartitionWideEntryOperation() {
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        SerializationService serializationService = getNodeEngine().getSerializationService();
        ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    public void run() {
        long now = getNow();

        Iterator<Record> iterator = recordStore.iterator(now, false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = record.getKey();
            Object oldValue = record.getValue();

            if (!applyPredicate(dataKey, oldValue)) {
                continue;
            }

            Map.Entry entry = createMapEntry(dataKey, oldValue);
            Data response = process(entry);

            addToResponses(dataKey, response);

            // first call noOp, other if checks below depends on it.
            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemoved(entry, dataKey, oldValue, now)) {
                continue;
            }
            entryAddedOrUpdated(entry, dataKey, oldValue, now);

            evict();
        }
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        PartitionWideEntryBackupOperation backupOperation = null;
        if (backupProcessor != null) {
            backupOperation = new PartitionWideEntryBackupOperation(name, backupProcessor);
            backupOperation.setWanEventList(wanEventList);
        }
        return backupOperation;
    }

    private boolean applyPredicate(Data key, Object value) {
        Predicate predicate = getPredicate();

        if (predicate == null || TruePredicate.INSTANCE == predicate) {
            return true;
        }

        if (FalsePredicate.INSTANCE == predicate) {
            return false;
        }

        QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
        return getPredicate().apply(queryEntry);
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

}
