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

import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockWaitNotifyKey;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

public abstract class LockAwareOperation extends KeyBasedMapOperation implements BlockingOperation {

    protected LockAwareOperation() {
    }

    protected LockAwareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public LockAwareOperation(String name, Data dataKey, Data dataValue) {
        super(name, dataKey, dataValue);
    }

    @Override
    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    @Override
    public abstract void onWaitExpire();

    @Override
    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }
}
