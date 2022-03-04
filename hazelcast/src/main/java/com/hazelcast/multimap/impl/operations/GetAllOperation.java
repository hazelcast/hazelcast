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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.util.Collection;

public class GetAllOperation extends AbstractKeyBasedMultiMapOperation implements BlockingOperation, ReadonlyOperation {

    public GetAllOperation() {
    }

    public GetAllOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        Collection<MultiMapRecord> coll = null;
        if (multiMapValue != null) {
            multiMapValue.incrementHit();
            coll = multiMapValue.getCollection(executedLocally());
        }
        response = new MultiMapResponse(coll, getValueCollectionType(container));
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.GET_ALL;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        MultiMapContainer container = getOrCreateContainer();
        if (container.isTransactionallyLocked(dataKey)) {
            return !container.canAcquireLock(dataKey, getCallerUuid(), threadId);
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }
}
