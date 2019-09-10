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

package com.hazelcast.cp.internal.datastructures.unsafe.lock.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.lock.LockStoreImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

public class GetRemainingLeaseTimeOperation extends AbstractLockOperation implements ReadonlyOperation {

    public GetRemainingLeaseTimeOperation() {
    }

    public GetRemainingLeaseTimeOperation(ObjectNamespace namespace, Data key) {
        super(namespace, key, -1);
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.GET_REMAINING_LEASETIME;
    }

    @Override
    public void run() throws Exception {
        LockStoreImpl lockStore = getLockStore();
        response = lockStore.getRemainingLeaseTime(key);
    }
}
