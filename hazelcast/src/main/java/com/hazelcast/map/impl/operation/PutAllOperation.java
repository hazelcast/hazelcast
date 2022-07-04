/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Inserts the {@link MapEntries} for a single partition to the
 * local {@link com.hazelcast.map.impl.recordstore.RecordStore}.
 * <p>
 * Used to reduce the number of remote invocations
 * of an {@link IMap#putAll(Map)} or {@link IMap#setAll(Map)} call.
 */
public class PutAllOperation extends MapOperation
        implements PartitionAwareOperation, BackupAwareOperation, MutatingOperation, Versioned {

    private transient int currentIndex;
    private MapEntries mapEntries;
    private boolean triggerMapLoader;

    private transient boolean hasMapListener;
    private transient boolean hasWanReplication;
    private transient boolean hasBackups;
    private transient boolean hasInvalidation;

    private transient List backupPairs;
    private transient List<Data> invalidationKeys;

    public PutAllOperation() {
    }

    public PutAllOperation(String name, MapEntries mapEntries, boolean triggerMapLoader) {
        super(name);
        this.mapEntries = mapEntries;
        this.triggerMapLoader = triggerMapLoader;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        hasMapListener = mapEventPublisher.hasEventListener(name);
        hasWanReplication = mapContainer.isWanReplicationEnabled();
        hasBackups = hasBackups();
        hasInvalidation = mapContainer.hasInvalidationListener();

        if (hasBackups) {
            backupPairs = new ArrayList(2 * mapEntries.size());
        }
        if (hasInvalidation) {
            invalidationKeys = new ArrayList<>(mapEntries.size());
        }
    }

    @Override
    protected void runInternal() {
        // if currentIndex is not zero, this is a
        // continuation of the operation after a NativeOOME
        int size = mapEntries.size();
        while (currentIndex < size) {
            put(mapEntries.getKey(currentIndex), mapEntries.getValue(currentIndex));
            currentIndex++;
        }
    }

    // protected for testing purposes
    protected void put(Data dataKey, Data dataValue) {
        Object oldValue = putToRecordStore(dataKey, dataValue);

        dataValue = getValueOrPostProcessedValue(dataKey, dataValue);
        mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);

        if (hasMapListener) {
            EntryEventType eventType = (oldValue == null ? ADDED : UPDATED);
            mapEventPublisher.publishEvent(getCallerAddress(), name,
                    eventType, dataKey, oldValue, dataValue);
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

    private boolean hasBackups() {
        return (mapContainer.getTotalBackupCount() > 0);
    }

    /**
     * The method recordStore.put() tries to fetch the old value from
     * the MapStore, which can lead to a serious performance degradation
     * if loading from MapStore is expensive. Only call recordStore.put()
     * when requested by user and there are map listeners are registered.
     * Otherwise call recordStore.set()
     */
    private Object putToRecordStore(Data dataKey, Data dataValue) {
        if (triggerMapLoader && hasMapListener) {
            return recordStore.put(dataKey, dataValue, UNSET, UNSET);
        }
        recordStore.set(dataKey, dataValue, UNSET, UNSET);
        return null;
    }

    @Override
    protected void afterRunInternal() {
        invalidateNearCache(invalidationKeys);

        super.afterRunInternal();
    }

    private Data getValueOrPostProcessedValue(Data dataKey, Data dataValue) {
        if (!isPostProcessing(recordStore)) {
            return dataValue;
        }
        Record record = recordStore.getRecord(dataKey);
        return mapServiceContext.toData(record.getValue());
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return (hasBackups && !mapEntries.isEmpty());
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name,
                toBackupListByRemovingEvictedRecords(), false);
    }

    /**
     * Since records may get evicted on NOOME after
     * they have been put. We are re-checking
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
                toBackupList.add(recordStore.getExpirySystem().getExpiryMetadata(dataKey));
            }
        }
        return toBackupList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeBoolean(triggerMapLoader);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            triggerMapLoader = in.readBoolean();
        } else {
            triggerMapLoader = true;
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL;
    }
}
