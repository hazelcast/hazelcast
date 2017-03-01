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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class MergeOperation extends BasePutOperation {

    private MapMergePolicy mergePolicy;
    private EntryView<Data, Data> mergingEntry;
    private boolean merged;
    private Data mergingValue;
    private boolean disableWanReplicationEvent;

    public MergeOperation(String name, Data dataKey, EntryView<Data, Data> entryView,
                          MapMergePolicy policy) {
        this(name, dataKey, entryView, policy, false);
    }

    public MergeOperation(String name, Data dataKey, EntryView<Data, Data> entryView,
                          MapMergePolicy policy, boolean disableWanReplicationEvent) {
        super(name, dataKey, null);
        this.mergingEntry = entryView;
        this.mergePolicy = policy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public MergeOperation() {
    }

    @Override
    public void run() {
        Record oldRecord = recordStore.getRecord(dataKey);
        if (oldRecord != null) {
            dataOldValue = mapServiceContext.toData(oldRecord.getValue());
        }
        merged = recordStore.merge(dataKey, mergingEntry, mergePolicy);
        if (merged) {
            Record record = recordStore.getRecord(dataKey);
            if (record != null) {
                dataValue = mapServiceContext.toData(record.getValue());
                mergingValue = mapServiceContext.toData(mergingEntry.getValue());
            }
        }
    }

    @Override
    public Object getResponse() {
        return merged;
    }

    @Override
    public boolean shouldBackup() {
        return merged;
    }

    @Override
    public void afterRun() {
        if (merged) {
            mapServiceContext.interceptAfterPut(name, dataValue);
            mapEventPublisher.publishEvent(getCallerAddress(), name, EntryEventType.MERGED, dataKey, dataOldValue,
                    dataValue, mergingValue);
            invalidateNearCache(dataKey);
            evict(dataKey);
        }
    }

    @Override
    public Operation getBackupOperation() {
        if (dataValue == null) {
            return new RemoveBackupOperation(name, dataKey, false, disableWanReplicationEvent);
        } else {
            final Record record = recordStore.getRecord(dataKey);
            final RecordInfo replicationInfo = Records.buildRecordInfo(record);
            return new PutBackupOperation(name, dataKey, dataValue, replicationInfo, false, false, disableWanReplicationEvent);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mergingEntry);
        out.writeObject(mergePolicy);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mergingEntry = in.readObject();
        mergePolicy = in.readObject();
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MERGE;
    }
}
