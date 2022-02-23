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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.EVICTED;

public class EvictOperation extends LockAwareOperation implements MutatingOperation, BackupAwareOperation {

    private boolean evicted;
    private boolean asyncBackup;

    public EvictOperation(String name, Data dataKey, boolean asyncBackup) {
        super(name, dataKey);
        this.asyncBackup = asyncBackup;
    }

    public EvictOperation() {
    }

    @Override
    protected void runInternal() {
        dataValue = mapServiceContext.toData(recordStore.evict(dataKey, false));
        evicted = dataValue != null;
    }

    @Override
    protected void afterRunInternal() {
        if (!evicted) {
            return;
        }
        mapServiceContext.interceptAfterRemove(mapContainer.getInterceptorRegistry(), dataValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, EVICTED, dataKey, dataValue, null);
        invalidateNearCache(dataKey);
    }

    @Override
    public Object getResponse() {
        return evicted;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    public Operation getBackupOperation() {
        return new EvictBackupOperation(name, dataKey);
    }

    @Override
    public int getAsyncBackupCount() {
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        if (asyncBackup) {
            return mapContainer.getTotalBackupCount();
        }

        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        if (asyncBackup) {
            return 0;
        }
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldBackup() {
        return evicted;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(asyncBackup);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        asyncBackup = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.EVICT;
    }
}
