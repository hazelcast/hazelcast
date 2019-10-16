/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.MERGED;

/**
 * Contains multiple merge entries for split-brain
 * healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeOperation extends MapOperation
        implements PartitionAwareOperation, BackupAwareOperation {

    private boolean disableWanReplicationEvent;
    private List<MapMergeTypes> mergingEntries;
    private SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy;

    private transient boolean hasMapListener;
    private transient boolean hasWanReplication;
    private transient boolean hasBackups;
    private transient boolean hasInvalidation;

    private transient List<Data> invalidationKeys;
    private transient boolean hasMergedValues;

    private List backupRecordAndDataValuePairs;

    public MergeOperation() {
    }

    public MergeOperation(String name, List<MapMergeTypes> mergingEntries,
                          SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy,
                          boolean disableWanReplicationEvent) {
        super(name);
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    @Override
    protected boolean disableWanReplicationEvent() {
        return disableWanReplicationEvent;
    }

    @Override
    protected void runInternal() {
        hasMapListener = mapEventPublisher.hasEventListener(name);
        hasWanReplication = mapContainer.isWanReplicationEnabled() && !disableWanReplicationEvent;
        hasBackups = mapContainer.getTotalBackupCount() > 0;
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            backupRecordAndDataValuePairs = new ArrayList<>(mergingEntries.size() * 2);
        }
        if (hasInvalidation) {
            invalidationKeys = new ArrayList<>(mergingEntries.size());
        }

        for (MapMergeTypes mergingEntry : mergingEntries) {
            merge(mergingEntry);
        }
    }

    private void merge(MapMergeTypes mergingEntry) {
        Data dataKey = mergingEntry.getKey();
        Data oldValue = hasMapListener ? getValue(dataKey) : null;

        if (recordStore.merge(mergingEntry, mergePolicy, getCallerProvenance())) {
            hasMergedValues = true;
            Data dataValue = getValueOrPostProcessedValue(dataKey, getValue(dataKey));
            mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);

            if (hasMapListener) {
                mapEventPublisher.publishEvent(getCallerAddress(), name, MERGED, dataKey, oldValue, dataValue);
            }
            if (hasWanReplication) {
                publishWanUpdate(dataKey, dataValue);
            }
            if (hasBackups) {
                Record record = recordStore.getRecord(dataKey);
                if (record != null) {
                    // TODO what about backup of removed records?
                    backupRecordAndDataValuePairs.add(record);
                    backupRecordAndDataValuePairs.add(dataValue);
                }
            }
            evict(dataKey);
            if (hasInvalidation) {
                invalidationKeys.add(dataKey);
            }
        }
    }

    private Data getValueOrPostProcessedValue(Data dataKey, Data dataValue) {
        if (!isPostProcessing(recordStore)) {
            return dataValue;
        }
        Record record = recordStore.getRecord(dataKey);
        return mapServiceContext.toData(record.getValue());
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
        return hasBackups && !backupRecordAndDataValuePairs.isEmpty();
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
    protected void afterRunInternal() {
        invalidateNearCache(invalidationKeys);
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name, backupRecordAndDataValuePairs, disableWanReplicationEvent);
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
        mergingEntries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            MapMergeTypes mergingEntry = in.readObject();
            mergingEntries.add(mergingEntry);
        }
        mergePolicy = in.readObject();
        disableWanReplicationEvent = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MERGE;
    }
}
