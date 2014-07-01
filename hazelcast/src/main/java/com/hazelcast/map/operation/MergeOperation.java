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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordInfo;
import com.hazelcast.map.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import java.io.IOException;

public class MergeOperation extends BasePutOperation {

    private MapMergePolicy mergePolicy;
    private EntryView<Data, Data> mergingEntry;
    private boolean merged;

    public MergeOperation(String name, Data dataKey, EntryView<Data, Data> entryView, MapMergePolicy policy) {
        super(name, dataKey, null);
        mergingEntry = entryView;
        mergePolicy = policy;
    }

    public MergeOperation() {
    }

    public void run() {
        merged = recordStore.merge(dataKey, mergingEntry, mergePolicy);
        if (merged) {
            Record record = recordStore.getRecord(dataKey);
            if (record != null) {
                dataValue = mapService.getMapServiceContext().toData(record.getValue());
            }
        }
    }

    @Override
    public Object getResponse() {
        return merged;
    }


    public boolean shouldBackup() {
        return merged;
    }

    public void afterRun() {
        if (merged) {
            invalidateNearCaches();
        }
    }

    public Operation getBackupOperation() {
        if (dataValue == null) {
            return new RemoveBackupOperation(name, dataKey);
        } else {
            RecordInfo replicationInfo = Records.buildRecordInfo(recordStore.getRecord(dataKey));
            return new PutBackupOperation(name, dataKey, dataValue, replicationInfo);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
        mergePolicy = in.readObject();
    }

    @Override
    public String toString() {
        return "MergeOperation{" + name + "}";
    }

}
