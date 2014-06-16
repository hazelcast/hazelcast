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
import com.hazelcast.map.RecordStore;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import static com.hazelcast.map.MapService.SERVICE_NAME;

public class ClearOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    boolean shouldBackup = true;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    public void run() {
        // near-cache clear will be called multiple times by each clear operation,
        // but it's still preferred to send a separate operation to clear near-cache.
        mapService.clearNearCache(name);

        final RecordStore recordStore = mapService.getExistingRecordStore(getPartitionId(), name);
        //if there is no recordStore, then there is nothing to clear.
        if (recordStore == null) {
            shouldBackup = false;
            return;
        }
        recordStore.clear();
    }

    @Override
    public void afterRun() throws Exception {
        mapService.publishEvent(getCallerAddress(), name, EntryEventType.CLEARED, null, null, null);
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public int getSyncBackupCount() {
        return mapService.getMapContainer(name).getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapService.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    public Operation getBackupOperation() {
        ClearBackupOperation clearBackupOperation = new ClearBackupOperation(name);
        clearBackupOperation.setServiceName(SERVICE_NAME);
        return clearBackupOperation;
    }

    @Override
    public String toString() {
        return "ClearOperation{" +
                '}';
    }
}
