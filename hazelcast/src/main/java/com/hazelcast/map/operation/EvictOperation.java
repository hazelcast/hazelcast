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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class EvictOperation extends LockAwareOperation implements BackupAwareOperation {

    boolean evicted;
    boolean asyncBackup;

    public EvictOperation(String name, Data dataKey, boolean asyncBackup) {
        super(name, dataKey);
        this.asyncBackup = asyncBackup;
    }

    public EvictOperation() {
    }

    public void run() {
        dataValue = mapService.getMapServiceContext().toData(recordStore.evict(dataKey, false));
        evicted = dataValue != null;
    }

    @Override
    public Object getResponse() {
        return evicted;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey);
    }

    public int getAsyncBackupCount() {
        if (asyncBackup) {
            return mapService.getMapServiceContext().getMapContainer(name).getTotalBackupCount();
        } else {
            return mapService.getMapServiceContext().getMapContainer(name).getAsyncBackupCount();
        }
    }

    public int getSyncBackupCount() {
        if (asyncBackup) {
            return 0;
        } else {
            return mapService.getMapServiceContext().getMapContainer(name).getBackupCount();
        }
    }

    public boolean shouldBackup() {
        return evicted;
    }

    public void afterRun() {
        if (evicted) {
            mapService.getMapServiceContext().interceptAfterRemove(name, dataValue);
            EntryEventType eventType = EntryEventType.EVICTED;
            mapService.getMapServiceContext().getMapEventPublisher()
                    .publishEvent(getCallerAddress(), name, eventType, dataKey, dataValue, null);
            invalidateNearCaches();
        }
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
    public String toString() {
        return "EvictOperation{" + name + "}";
    }
}
