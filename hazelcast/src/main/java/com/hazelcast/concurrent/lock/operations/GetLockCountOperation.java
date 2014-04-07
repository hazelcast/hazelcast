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

package com.hazelcast.concurrent.lock.operations;

import com.hazelcast.concurrent.lock.LockDataSerializerHook;
import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;

public class GetLockCountOperation extends BaseLockOperation {

    public GetLockCountOperation() {
    }

    public GetLockCountOperation(ObjectNamespace namespace, Data key) {
        super(namespace, key, -1);
    }

    @Override
    public int getId() {
        return LockDataSerializerHook.GET_LOCK_COUNT;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        response = lockStore.getLockCount(key);
    }
}
