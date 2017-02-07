/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class PartitionWideEntryBackupOperation extends AbstractMultipleEntryBackupOperation implements BackupOperation {

    public PartitionWideEntryBackupOperation() {
    }

    public PartitionWideEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    public void run() {
        long now = getNow();
        boolean shouldClone = mapContainer.shouldCloneOnEntryProcessing();
        SerializationService serializationService = getNodeEngine().getSerializationService();

        Iterator<Record> iterator = recordStore.iterator(now, true);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = record.getKey();
            Object oldValue = record.getValue();
            Object value = shouldClone ? serializationService.toObject(serializationService.toData(oldValue)) : oldValue;

            if (!applyPredicate(dataKey, value)) {
                continue;
            }

            Map.Entry entry = createMapEntry(dataKey, value);
            processBackup(entry);

            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemovedBackup(entry, dataKey)) {
                continue;
            }
            entryAddedOrUpdatedBackup(entry, dataKey);

            evict(dataKey);
        }

        publishWanReplicationEventBackups();
    }

    protected Predicate getPredicate() {
        return null;
    }

    @Override
    public Object getResponse() {
        return true;
    }

    private boolean applyPredicate(Data key, Object value) {
        if (getPredicate() == null) {
            return true;
        }
        QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
        return getPredicate().apply(queryEntry);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PARTITION_WIDE_ENTRY_BACKUP;
    }
}
