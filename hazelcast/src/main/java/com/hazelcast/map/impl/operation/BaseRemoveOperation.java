/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

public abstract class BaseRemoveOperation extends LockAwareOperation
        implements BackupAwareOperation, MutatingOperation {

    protected transient Data dataOldValue;

    public BaseRemoveOperation(String name, Data dataKey, boolean disableWanReplicationEvent) {
        super(name, dataKey);
        this.disableWanReplicationEvent = disableWanReplicationEvent;
    }

    public BaseRemoveOperation(String name, Data dataKey) {
        this(name, dataKey, false);
    }

    public BaseRemoveOperation() {
    }

    @Override
    protected void afterRunInternal() {
        mapServiceContext.interceptAfterRemove(name, dataOldValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name,
                EntryEventType.REMOVED, dataKey, dataOldValue, null);
        invalidateNearCache(dataKey);
        publishWanRemove(dataKey);
        evict(dataKey);
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey,
                false, disableWanReplicationEvent);
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }
}
