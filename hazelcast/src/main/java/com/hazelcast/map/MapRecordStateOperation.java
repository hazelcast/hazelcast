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

package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MapStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;

public class MapRecordStateOperation extends LockAwareOperation implements BackupAwareOperation {


    PartitionContainer partitionContainer;
    ResponseHandler responseHandler;
    RecordStore recordStore;
    MapService mapService;
    NodeEngine nodeEngine;
    boolean evicted = false;

    public MapRecordStateOperation(String name, Data dataKey) {
        super(name, dataKey, -1);
    }

    protected void init() {
        responseHandler = getResponseHandler();
        mapService = getService();
        nodeEngine = getNodeEngine();
        partitionContainer = mapService.getPartitionContainer(getPartitionId());
        recordStore = partitionContainer.getRecordStore(name);
    }


    public MapRecordStateOperation() {
    }

    @Override
    public void run() {
        Record record = recordStore.getRecords().get(dataKey);
        if (record != null) {
            if (record.getState().isDirty()) {
                MapStore store = recordStore.getMapInfo().getStore();
                if (store != null) {
                    Object value = record.getValue();
                    store.store(mapService.toObject(dataKey), mapService.toObject(value));
                }
                record.getState().resetStoreTime();
            }

            if (record.getState().isExpired()) {
                record.getState().resetExpiration();
                dataValue = mapService.toData(record.getValue());
                recordStore.evict(dataKey);
                evicted = true;
            }
        } else if (recordStore.getRemovedDelayedKeys().contains(dataKey)) {
            MapStore store = recordStore.getMapInfo().getStore();
            if (store != null) {
                store.delete(nodeEngine.getSerializationService().toObject(dataKey));
            }
            recordStore.getRemovedDelayedKeys().remove(dataKey);
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public void onWaitExpire() {
    }


    public void beforeRun() {
        init();
    }

    public void afterRun() {
        if (evicted) {
            int eventType = EntryEvent.TYPE_EVICTED;
            mapService.publishEvent(getCaller(), name, eventType, dataKey, null, dataValue);
        }
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey);
    }

    public int getAsyncBackupCount() {
        return mapService.getMapInfo(name).getAsyncBackupCount();
    }

    public boolean shouldBackup() {
        return evicted;
    }

    public int getSyncBackupCount() {
        return mapService.getMapInfo(name).getBackupCount();
    }

    @Override
    public String toString() {
        return "MapRecordStateOperation{" + name + "}";
    }
}
