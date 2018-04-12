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

import com.hazelcast.core.EntryView;
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
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.MERGED;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Merges multiple {@link MapMergeTypes} for split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends MapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private List<MapMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<Object, MapMergeTypes> mergePolicy;
    private boolean disableWanReplicationEvent;

    private transient boolean hasMapListener;
    private transient boolean hasWanReplication;
    private transient boolean hasBackups;
    private transient boolean hasInvalidation;

    private transient MapEntries mapEntries;
    private transient List<RecordInfo> backupRecordInfos;
    private transient List<Data> invalidationKeys;
    private transient boolean hasMergedValues;

    public MergeOperation() {
    }

    MergeOperation(String name, List<MapMergeTypes> mergingEntries, SplitBrainMergePolicy<Object, MapMergeTypes> mergePolicy,
                   boolean disableWanReplicationEvent) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    public void run() {
        hasMapListener = mapEventPublisher.hasEventListener(name);
        hasWanReplication = mapContainer.isWanReplicationEnabled() && !disableWanReplicationEvent;
        hasBackups = mapContainer.getTotalBackupCount() > 0;
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            mapEntries = new MapEntries(mergingEntries.size());
            backupRecordInfos = new ArrayList<RecordInfo>(mergingEntries.size());
        }
        if (hasInvalidation) {
            invalidationKeys = new ArrayList<Data>(mergingEntries.size());
        }

        for (MapMergeTypes mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }
    }

    private void merge(MapMergeTypes mergingEntry) {
        Data dataKey = mergingEntry.getKey();
        Data oldValue = hasMapListener ? getValue(dataKey) : null;

        if (recordStore.merge(mergingEntry, mergePolicy)) {
            hasMergedValues = true;
            Data newValue = getValue(dataKey);
            mapServiceContext.interceptAfterPut(name, newValue);

            publishListenerEvent(dataKey, oldValue, newValue);
            publishWanReplication(dataKey, newValue);
            if (hasBackups) {
                mapEntries.add(dataKey, newValue);
                backupRecordInfos.add(newValue == null ? null : buildRecordInfo(recordStore.getRecord(dataKey)));
            }
            evict(dataKey);
            if (hasInvalidation) {
                invalidationKeys.add(dataKey);
            }
        }
    }

    private void publishListenerEvent(Data dataKey, Data oldValue, Data dataValue) {
        if (hasMapListener) {
            mapEventPublisher.publishEvent(getCallerAddress(), name, MERGED, dataKey, oldValue, dataValue);
        }
    }

    private void publishWanReplication(Data dataKey, Data dataValue) {
        if (hasWanReplication) {
            if (dataValue == null) {
                mapEventPublisher.publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
            } else {
                EntryView<Data, Data> entryView = createSimpleEntryView(dataKey, dataValue, recordStore.getRecord(dataKey));
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }
    }

    private Data getValue(Data dataKey) {
        Record record = recordStore.getRecord(dataKey);
        if (record != null) {
            return mapServiceContext.toData(record.getValue());
        }
        return null;
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
    public void afterRun() throws Exception {
        invalidateNearCache(invalidationKeys);

        super.afterRun();
    }

    @Override
    public Operation getBackupOperation() {
        return new MergeBackupOperation(name, mapEntries, backupRecordInfos, hasWanReplication);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(mergingEntries.size());
        for (MapMergeTypes mergingEntry : mergingEntries) {
            out.writeObject(mergingEntry);
        }
        out.writeObject(mergePolicy);
        out.writeBoolean(disableWanReplicationEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        mergingEntries = new ArrayList<MapMergeTypes>(size);
        for (int i = 0; i < size; i++) {
            MapMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MERGE;
    }
}
