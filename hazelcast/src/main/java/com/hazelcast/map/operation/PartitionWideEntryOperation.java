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
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * There are 2 problems:
 * 1: how to correctly continue where you left the previous time
 * 2: how to with the response; especially if the response for one partition is build up during an interleaved migration
 *
 * todo: PartitionWideEntryBackupOperation needs to be fixed.
 */
public class PartitionWideEntryOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    EntryProcessor entryProcessor;
    MapEntrySet response;
    //todo: do we want size based or time based batching?
    private int batchSize = 10;
    private Iterator<Map.Entry<Data, Record>> iterator;
    private RecordStore recordStore;
    private int processed = 0;
    public PartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    public PartitionWideEntryOperation() {
    }

    @Override
    public void run() {
        if(iterator == null){
            recordStore = mapService.getRecordStore(getPartitionId(), name);
            iterator = recordStore.getRecords().entrySet().iterator();
            response = new MapEntrySet();
        }

        for (int k=0;k<batchSize;k++) {
            if(!iterator.hasNext()){
                break;
            }

            processed++;
             Map.Entry<Data,Record> recordEntry = iterator.next();
            Data dataKey = recordEntry.getKey();
            Record record = recordEntry.getValue();
            Map.Entry entry = new AbstractMap.SimpleEntry(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()));
            //todo: we probably want some protection here against exceptions.
            Object result = entryProcessor.process(entry);
            if (result != null) {
                response.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, mapService.toData(result)));
            }

            if (entry.getValue() == null) {
                recordStore.remove(dataKey);
            } else {
                recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, entry.getValue()));
            }
        }

        if(iterator.hasNext()){
            this.getNodeEngine().getOperationService().executeOperation(this);
            System.out.println("Posting next batch");
        } else{
            if(processed>0){
                System.out.println("Completed:"+processed);
            }
        }
    }

    @Override
    public boolean returnsResponse() {
        if(iterator == null)return false;

        return !iterator.hasNext();
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
        return backupProcessor != null ? new PartitionWideEntryBackupOperation(name, backupProcessor) : null;
    }
}
