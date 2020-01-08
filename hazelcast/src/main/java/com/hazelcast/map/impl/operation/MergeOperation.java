/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import javax.annotation.Nonnull;
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
    private List<MapMergeTypes<Object, Object>> mergingEntries;
    private SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy;

    private transient int currentIndex;
    private transient boolean hasMapListener;
    private transient boolean hasWanReplication;
    private transient boolean hasBackups;
    private transient boolean hasInvalidation;

    private transient List<Data> invalidationKeys;
    private transient boolean hasMergedValues;

    private List backupPairs;

    public MergeOperation() {
    }

    public MergeOperation(String name, List<MapMergeTypes<Object, Object>> mergingEntries,
                          SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy,
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
        hasWanReplication = mapContainer.isWanReplicationEnabled()
                && !disableWanReplicationEvent;
        hasBackups = mapContainer.getTotalBackupCount() > 0;
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            backupPairs = new ArrayList(2 * mergingEntries.size());
        }

        if (hasInvalidation) {
            invalidationKeys = new ArrayList<>(mergingEntries.size());
        }

        // if currentIndex is not zero, this is a
        // continuation of the operation after a NativeOOME
        int size = mergingEntries.size();
        while (currentIndex < size) {
            merge(mergingEntries.get(currentIndex));
            currentIndex++;
        }
    }

    private void merge(MapMergeTypes<Object, Object> mergingEntry) {
        Data dataKey = getNodeEngine().toData(mergingEntry.getRawKey());
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

            if (hasInvalidation) {
                invalidationKeys.add(dataKey);
            }

            if (hasBackups) {
                backupPairs.add(dataKey);
                backupPairs.add(dataValue);
            }

            evict(dataKey);
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
        return hasBackups && !backupPairs.isEmpty();
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

        super.afterRunInternal();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name,
                toBackupListByRemovingEvictedRecords(), disableWanReplicationEvent);
    }

    /**
     * Since records may get evicted on NOOME after
     * they have been merged. We are re-checking
     * backup pair list to eliminate evicted entries.
     *
     * @return list of existing records which can
     * safely be transferred to backup replica.
     */
    @Nonnull
    private List toBackupListByRemovingEvictedRecords() {
        List toBackupList = new ArrayList(backupPairs.size());
        for (int i = 0; i < backupPairs.size(); i += 2) {
            Data dataKey = ((Data) backupPairs.get(i));
            Record record = recordStore.getRecord(dataKey);
            if (record != null) {
                toBackupList.add(dataKey);
                toBackupList.add(backupPairs.get(i + 1));
                toBackupList.add(record);
            }
        }
        return toBackupList;
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
