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

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

public abstract class LockAwareOperation extends KeyBasedMapOperation implements WaitSupport {

    protected LockAwareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    protected LockAwareOperation(String name, Data dataKey, long ttl) {
        super(name, dataKey, ttl);
    }

    protected LockAwareOperation(String name, Data dataKey, Data dataValue, long ttl) {
        super(name, dataKey, dataValue, ttl);
    }

    protected LockAwareOperation() {
    }

    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    public abstract void onWaitExpire();

    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }
}
