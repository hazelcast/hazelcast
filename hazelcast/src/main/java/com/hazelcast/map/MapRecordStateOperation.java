/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MapStore;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.RecordState;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;

public class MapRecordStateOperation extends LockAwareOperation implements BackupAwareOperation {


    PartitionContainer partitionContainer;
    ResponseHandler responseHandler;
    RecordStore recordStore;
    MapService mapService;
    NodeEngine nodeEngine;
    Operation backupOperation = null;

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
    void doOp() {
        Record record = recordStore.getRecords().get(dataKey);
        if( record != null ) {
            if(record.getState().isDirty()) {
                MapStore store = recordStore.getMapInfo().getStore();
                if(store != null) {
                    store.store(dataKey, record.getValue());
                }
                record.getState().resetStoreTime();
            }
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
    }


    public Operation getBackupOperation() {
        return backupOperation;
    }

    public int getAsyncBackupCount() {
        return mapService.getMapInfo(name).getAsyncBackupCount();
    }

    public boolean shouldBackup() {
        return backupOperation != null;
    }

    public int getSyncBackupCount() {
        return mapService.getMapInfo(name).getBackupCount();
    }

    @Override
    public String toString() {
        return "MapRecordStateOperation{" + name + "}";
    }
}
