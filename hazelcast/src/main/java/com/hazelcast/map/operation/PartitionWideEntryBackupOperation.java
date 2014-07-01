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

package com.hazelcast.map.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

public class PartitionWideEntryBackupOperation extends AbstractMapOperation implements BackupOperation, PartitionAwareOperation {

    EntryBackupProcessor entryProcessor;

    public PartitionWideEntryBackupOperation(String name, EntryBackupProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    public PartitionWideEntryBackupOperation() {
    }

    public void run() {
        Map.Entry entry;
        RecordStore recordStore = mapService.getMapServiceContext().getRecordStore(getPartitionId(), name);
        Map<Data, Record> records = recordStore.getReadonlyRecordMap();
        for (Map.Entry<Data, Record> recordEntry : records.entrySet()) {
            Data dataKey = recordEntry.getKey();
            Record record = recordEntry.getValue();
            Object objectKey = mapService.getMapServiceContext().toObject(record.getKey());
            Object valueBeforeProcess = mapService.getMapServiceContext().toObject(record.getValue());
            if (getPredicate() != null) {
                QueryEntry queryEntry = new QueryEntry(getNodeEngine().getSerializationService(), dataKey, objectKey, valueBeforeProcess);
                if (!getPredicate().apply(queryEntry)) {
                    continue;
                }
            }
            entry = new AbstractMap.SimpleEntry(objectKey, valueBeforeProcess);
            entryProcessor.processBackup(entry);
            if (entry.getValue() == null) {
                recordStore.removeBackup(dataKey);
            } else {
                recordStore.putBackup(dataKey, entry.getValue());
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
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

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "PartitionWideEntryBackupOperation{}";
    }
}
