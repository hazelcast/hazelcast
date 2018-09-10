/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * Contains a merging entry for split-brain healing with a {@link MapMergePolicy}.
 */
public class LegacyMergeOperation extends BasePutOperation {

    private MapMergePolicy mergePolicy;
    private EntryView<Data, Data> mergingEntry;

    private transient boolean merged;

    public LegacyMergeOperation() {
    }

    public LegacyMergeOperation(String name, EntryView<Data, Data> mergingEntry, MapMergePolicy policy,
                                boolean disableWanReplicationEvent) {
        super(name, mergingEntry.getKey(), null);
        this.mergingEntry = mergingEntry;
        this.mergePolicy = policy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    public void run() {
        Record oldRecord = recordStore.getRecord(dataKey);
        if (oldRecord != null) {
            dataOldValue = mapServiceContext.toData(oldRecord.getValue());
        }
        merged = recordStore.merge(dataKey, mergingEntry, mergePolicy, getCallerProvenance());
        if (merged) {
            Record record = recordStore.getRecord(dataKey);
            if (record != null) {
                dataValue = mapServiceContext.toData(record.getValue());
                dataMergingValue = mapServiceContext.toData(mergingEntry.getValue());
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
            eventType = EntryEventType.MERGED;
            super.afterRun();
        }
    }

    @Override
    public Operation getBackupOperation() {
        if (dataValue == null) {
            return new RemoveBackupOperation(name, dataKey, false, disableWanReplicationEvent);
        } else {
           return super.getBackupOperation();
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
        return MapDataSerializerHook.LEGACY_MERGE;
    }
}
