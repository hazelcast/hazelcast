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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

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
    public void innerBeforeRun() {
        super.innerBeforeRun();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    public void run() {
        final long now = getNow();

        final Iterator<Record> iterator = recordStore.iterator(now, false);
        while (iterator.hasNext()) {
            final Record record = iterator.next();
            final Data dataKey = record.getKey();
            final Object oldValue = record.getValue();

            final Object key = toObject(dataKey);
            final Object value = toObject(oldValue);

            if (!applyPredicate(dataKey, key, value)) {
                continue;
            }

            final Map.Entry entry = createMapEntry(key, value);

            final Data response = process(entry);

            addToResponses(dataKey, response);

            // first call noOp, other if checks below depends on it.
            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemoved(entry, dataKey, oldValue, now)) {
                continue;
            }
            entryAddedOrUpdated(entry, dataKey, oldValue, now);
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return responses;
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

    @Override
    public String toString() {
        return "PartitionWideEntryOperation{}";
    }

    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    public int getSyncBackupCount() {
        return 0;
    }

    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new PartitionWideEntryBackupOperation(name, backupProcessor) : null;
    }

    private boolean applyPredicate(Data dataKey, Object key, Object value) {
        if (getPredicate() == null) {
            return true;
        }
        final SerializationService ss = getNodeEngine().getSerializationService();
        QueryEntry queryEntry = new QueryEntry(ss, dataKey, key, value);
        return getPredicate().apply(queryEntry);
    }

    protected Predicate getPredicate() {
        return null;
    }
}
