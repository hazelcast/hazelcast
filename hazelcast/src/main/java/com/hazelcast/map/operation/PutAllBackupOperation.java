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

import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class PutAllBackupOperation extends AbstractMapOperation implements PartitionAwareOperation, BackupOperation {

    private MapEntrySet entrySet;
    private RecordStore recordStore;

    public PutAllBackupOperation(String name, MapEntrySet entrySet) {
        super(name);
        this.entrySet = entrySet;
    }

    public PutAllBackupOperation() {
    }

    public void run() {
        int partitionId = getPartitionId();
        recordStore = mapService.getRecordStore(partitionId, name);
        Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
        for (Map.Entry<Data, Data> entry : entries) {
            Data dataKey = entry.getKey();
            Data dataValue = entry.getValue();
            Record record = recordStore.getRecord(dataKey);
            if (record == null) {
                record = mapService.createRecord(name, dataKey, dataValue, -1, false);
                updateSizeEstimator(calculateRecordSize(record));
                recordStore.putRecord(dataKey, record);
            } else {
                updateSizeEstimator(-calculateRecordSize(record));
                mapContainer.getRecordFactory().setValue(record, dataValue);
                updateSizeEstimator(calculateRecordSize(record));
            }
        }
    }

    private void updateSizeEstimator( long recordSize ) {
        recordStore.getSizeEstimator().add( recordSize );
    }

    private long calculateRecordSize( Record record ) {
        return recordStore.getSizeEstimator().getCost(record);
    }

    @Override
    public Object getResponse() {
        return entrySet;
    }

    @Override
    public String toString() {
        return "PutAllBackupOperation{" +
                '}';
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entrySet);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entrySet = in.readObject();
    }

}
