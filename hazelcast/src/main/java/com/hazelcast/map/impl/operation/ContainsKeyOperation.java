/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;

public class ContainsKeyOperation extends ReadonlyKeyBasedMapOperation implements BlockingOperation {

    private transient boolean containsKey;

    public ContainsKeyOperation() {
    }

    public ContainsKeyOperation(String name, Data dataKey) {
        this.name = name;
        this.dataKey = dataKey;
    }

    @Override
    public void run() {
        containsKey = recordStore.containsKey(dataKey);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.CONTAINS_KEY;
    }

    @Override
    public Object getResponse() {
        return containsKey;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        DistributedObjectNamespace namespace = new DistributedObjectNamespace(MapService.SERVICE_NAME, name);
        return new LockWaitNotifyKey(namespace, dataKey);
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
}
