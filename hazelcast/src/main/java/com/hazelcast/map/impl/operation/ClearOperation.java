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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class ClearOperation extends MapOperation
        implements BackupAwareOperation, PartitionAwareOperation, MutatingOperation {

    private boolean shouldBackup;
    private int numberOfClearedEntries;

    public ClearOperation() {
        this(null);
    }

    public ClearOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
    }

    @Override
    protected void runInternal() {
        if (recordStore == null) {
            return;
        }

        numberOfClearedEntries = recordStore.clear();
        shouldBackup = true;
    }

    @Override
    protected void afterRunInternal() {
        invalidateAllKeysInNearCaches();
        hintMapEvent();
        super.afterRunInternal();
    }

    private void hintMapEvent() {
        mapEventPublisher.hintMapEvent(getCallerAddress(), name, EntryEventType.CLEAR_ALL,
                numberOfClearedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public int getSyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Object getResponse() {
        return numberOfClearedEntries;
    }

    public Operation getBackupOperation() {
        ClearBackupOperation clearBackupOperation = new ClearBackupOperation(name);
        clearBackupOperation.setServiceName(SERVICE_NAME);
        return clearBackupOperation;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.CLEAR;
    }
}
