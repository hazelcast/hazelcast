/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.operation.steps.ClearOpSteps;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

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

        numberOfClearedEntries = recordStore.clear(false);
        shouldBackup = true;
    }

    @Override
    public Step getStartingStep() {
        return ClearOpSteps.CLEAR_MEMORY;
    }

    @Override
    public void applyState(State state) {
        if (recordStore == null) {
            return;
        }
        super.applyState(state);
        numberOfClearedEntries = (int) state.getResult();
        shouldBackup = true;
    }

    @Override
    public void afterRunInternal() {
        if (mapContainer != null) {
            invalidateAllKeysInNearCaches();
        }
        hintMapEvent();
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

    @Override
    public Operation getBackupOperation() {
        return new ClearBackupOperation(name)
                .setServiceName(SERVICE_NAME);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.CLEAR;
    }
}
