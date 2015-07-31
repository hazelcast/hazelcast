/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryView;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

public class GetEntryViewOperation extends KeyBasedMapOperation implements ReadonlyOperation, WaitSupport {

    private EntryView<Data, Data> result;

    public GetEntryViewOperation() {
    }

    public GetEntryViewOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    public void run() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record != null) {
            Data value = mapServiceContext.toData(record.getValue());
            result = EntryViews.createSimpleEntryView(record.getKey(), value, record);
        }
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    public boolean shouldWait() {
        return recordStore.isTransactionallyLocked(dataKey)
                && !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    @Override
    public void onWaitExpire() {
        ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public String toString() {
        return "GetEntryViewOperation{}";
    }
}
