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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.EntryViews;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapEventPublisher;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.NearCacheProvider;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordInfo;
import com.hazelcast.map.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PutAllOperation extends AbstractMapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private MapEntrySet entrySet;
    private boolean initialLoad;
    private List<Map.Entry<Data, Data>> backupEntrySet;
    private List<RecordInfo> backupRecordInfos;

    public PutAllOperation(String name, MapEntrySet entrySet) {
        super(name);
        this.entrySet = entrySet;
    }

    public PutAllOperation(String name, MapEntrySet entrySet, boolean initialLoad) {
        super(name);
        this.entrySet = entrySet;
        this.initialLoad = initialLoad;
    }

    public PutAllOperation() {
    }

    public void run() {
        backupRecordInfos = new ArrayList<RecordInfo>();
        backupEntrySet = new ArrayList<Map.Entry<Data, Data>>();
        int partitionId = getPartitionId();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, name);
        Set<Map.Entry<Data, Data>> entries = entrySet.getEntrySet();
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        Set<Data> keysToInvalidate = new HashSet<Data>();
        for (Map.Entry<Data, Data> entry : entries) {
            Data dataKey = entry.getKey();
            Data dataValue = entry.getValue();
            if (partitionId == partitionService.getPartitionId(dataKey)) {
                Data dataOldValue = null;
                if (initialLoad) {
                    recordStore.putFromLoad(dataKey, dataValue, -1);
                } else {
                    dataOldValue = mapServiceContext.toData(recordStore.put(dataKey, dataValue, -1));
                }
                mapServiceContext.interceptAfterPut(name, dataValue);
                EntryEventType eventType = dataOldValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
                final MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
                mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, dataOldValue, dataValue);
                keysToInvalidate.add(dataKey);

                // check in case of an expiration.
                final Record record = recordStore.getRecord(dataKey);
                if (record == null) {
                    continue;
                }
                if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
                    if (record != null) {
                        final Data dataValueAsData = mapServiceContext.toData(dataValue);
                        final EntryView entryView = EntryViews.createSimpleEntryView(dataKey, dataValueAsData, record);
                        mapEventPublisher.publishWanReplicationUpdate(name, entryView);
                    }
                }
                backupEntrySet.add(entry);
                RecordInfo replicationInfo = Records.buildRecordInfo(recordStore.getRecord(dataKey));
                backupRecordInfos.add(replicationInfo);
            }
        }
        invalidateNearCaches(keysToInvalidate);
    }

    protected final void invalidateNearCaches(Set<Data> keys) {
        final NearCacheProvider nearCacheProvider = mapService.getMapServiceContext().getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(name)) {
            nearCacheProvider.invalidateAllNearCaches(name, keys);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public String toString() {
        return "PutAllOperation{"
                + '}';

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entrySet);
        out.writeBoolean(initialLoad);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entrySet = in.readObject();
        initialLoad = in.readBoolean();
    }

    @Override
    public boolean shouldBackup() {
        return !backupEntrySet.isEmpty();
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name, backupEntrySet, backupRecordInfos);
    }
}
