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
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

public class PartitionWideEntryOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    EntryProcessor entryProcessor;
    MapEntrySet response;

    public PartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    public PartitionWideEntryOperation() {
    }

    public void run() {
        response = new MapEntrySet();
        Map.Entry entry;
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        Map<Data, Record> records = recordStore.getRecords();
        for (Map.Entry<Data, Record> recordEntry : records.entrySet()) {
            Data dataKey = recordEntry.getKey();
            Record record = recordEntry.getValue();
            entry = new AbstractMap.SimpleEntry(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()));
            Object result = entryProcessor.process(entry);
            if (result != null)
                response.add(new AbstractMap.SimpleImmutableEntry<Data,Data>(dataKey, mapService.toData(result)));
            if(entry.getValue() == null) {
                recordStore.remove(dataKey);
            }
            else {
                recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, entry.getValue()));
            }
        }
    }

    public void afterRun() throws Exception {
        super.afterRun();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
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
}
