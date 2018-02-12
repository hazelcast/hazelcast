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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Contains multiple merge entries for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends MapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private List<SplitBrainMergeEntryView<Data, Data>> mergeEntries;
    private SplitBrainMergePolicy mergePolicy;
    private boolean disableWanReplicationEvent;

    private transient boolean hasBackups;
    private transient boolean hasMergedValues;

    private transient MapEntries mapEntries;
    private transient List<RecordInfo> backupRecordInfos;

    public MergeOperation() {
    }

    MergeOperation(String name, List<SplitBrainMergeEntryView<Data, Data>> mergeEntries, SplitBrainMergePolicy policy,
                   boolean disableWanReplicationEvent) {
        super(name);
        this.mergeEntries = mergeEntries;
        this.mergePolicy = policy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();

        hasBackups = mapContainer.getTotalBackupCount() > 0;
        if (hasBackups) {
            mapEntries = new MapEntries(mergeEntries.size());
            backupRecordInfos = new ArrayList<RecordInfo>(mergeEntries.size());
        }
    }

    @Override
    public void run() {
        for (SplitBrainMergeEntryView<Data, Data> mergingEntry : mergeEntries) {
            Boolean merged = recordStore.merge(mergingEntry, mergePolicy,
                    !disableWanReplicationEvent, getCallerUuid(), getCallerAddress());
            hasMergedValues = merged ? true : hasMergedValues;
            if (merged && hasBackups) {
                Data dataKey = mergingEntry.getKey();
                Record record = recordStore.getRecord(dataKey);

                mapEntries.add(dataKey, mapServiceContext.toData(record.getValue()));
                backupRecordInfos.add(buildRecordInfo(record));
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        // intentionally empty body
    }

    @Override
    public Object getResponse() {
        return hasMergedValues;
    }

    @Override
    public boolean shouldBackup() {
        return hasBackups && !backupRecordInfos.isEmpty();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name, mapEntries, backupRecordInfos);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergeEntries.size());
        for (SplitBrainMergeEntryView<Data, Data> mergeEntry : mergeEntries) {
            out.writeObject(mergeEntry);
        }
        out.writeObject(mergePolicy);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergeEntries = new ArrayList<SplitBrainMergeEntryView<Data, Data>>();
        for (int i = 0; i < size; i++) {
            SplitBrainMergeEntryView<Data, Data> mergeEntry = in.readObject();
            mergeEntries.add(mergeEntry);
        }
        mergePolicy = in.readObject();
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MERGE;
    }
}
