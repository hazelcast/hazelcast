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
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.Clock;

public abstract class BaseRemoveOperation extends LockAwareOperation implements BackupAwareOperation {

    protected transient Data dataOldValue;

    public BaseRemoveOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public BaseRemoveOperation() {
    }

    public void afterRun() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        mapServiceContext.interceptAfterRemove(name, dataValue);
        mapServiceContext.getMapEventPublisher().publishEvent(getCallerAddress(), name, EntryEventType.REMOVED, dataKey, dataOldValue, null);
        invalidateNearCaches();
        if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
            // todo should evict operation replicated??
            mapServiceContext.getMapEventPublisher().publishWanReplicationRemove(name, dataKey, Clock.currentTimeMillis());
        }
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey);
    }

    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    public boolean shouldBackup() {
        return true;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    @Override
    public String toString() {
        return "BaseRemoveOperation{" + name + "}";
    }
}
