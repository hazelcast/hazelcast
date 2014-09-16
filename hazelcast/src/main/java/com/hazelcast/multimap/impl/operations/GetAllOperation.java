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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapWrapper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import java.util.Collection;

public class GetAllOperation extends MultiMapKeyBasedOperation implements WaitSupport {

    public GetAllOperation() {
    }

    public GetAllOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        MultiMapWrapper wrapper = container.getMultiMapWrapper(dataKey);
        Collection coll = null;
        if (wrapper != null) {
            wrapper.incrementHit();
            final ResponseHandler responseHandler = getResponseHandler();
            coll = wrapper.getCollection(responseHandler.isLocal());
        }
        response = new MultiMapResponse(coll, getValueCollectionType(container));
    }

    public int getId() {
        return MultiMapDataSerializerHook.GET_ALL;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        MultiMapContainer container = getOrCreateContainer();
        if (container.isTransactionallyLocked(dataKey)) {
            return !container.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }
}
