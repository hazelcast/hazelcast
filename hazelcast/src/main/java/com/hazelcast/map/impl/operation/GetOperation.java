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

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

public final class GetOperation extends ReadonlyKeyBasedMapOperation implements BlockingOperation {

    private Data result;

    public GetOperation() {
    }

    public GetOperation(String name, Data dataKey) {
        super(name, dataKey);

        this.dataKey = dataKey;
    }

    @Override
    protected void runInternal() {
        Object currentValue = recordStore.get(dataKey, false, getCallerAddress());
        if (!executedLocally() && currentValue instanceof Data) {
            // in case of a 'remote' call (e..g a client call) we prevent making an onheap copy of the offheap data
            result = (Data) currentValue;
        } else {
            // in case of a local call, we do make a copy so we can safely share it with e.g. near cache invalidation
            result = mapService.getMapServiceContext().toData(currentValue);
        }
    }

    @Override
    protected void afterRunInternal() {
        mapServiceContext.interceptAfterGet(mapContainer.getInterceptorRegistry(), result);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    public boolean shouldWait() {
        if (recordStore.isTransactionallyLocked(dataKey)) {
            return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }

    @Override
    public Data getResponse() {
        return result;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.GET;
    }
}
