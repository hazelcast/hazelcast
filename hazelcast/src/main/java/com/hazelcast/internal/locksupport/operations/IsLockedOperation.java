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

package com.hazelcast.internal.locksupport.operations;

import com.hazelcast.internal.locksupport.LockStoreImpl;
import com.hazelcast.internal.locksupport.LockDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

public class IsLockedOperation extends AbstractLockOperation implements ReadonlyOperation {

    public IsLockedOperation() {
    }

    public IsLockedOperation(ObjectNamespace namespace, Data key) {
        super(namespace, key, ANY_THREAD);
    }

    public IsLockedOperation(ObjectNamespace namespace, Data key, long threadId) {
        super(namespace, key, threadId);
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.IS_LOCKED;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        if (threadId == ANY_THREAD) {
            response = lockStore.isLocked(key);
        } else {
            response = lockStore.isLockedBy(key, getCallerUuid(), threadId);
        }
    }
}
