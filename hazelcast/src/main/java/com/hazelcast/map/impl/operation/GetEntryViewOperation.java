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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

public class GetEntryViewOperation extends ReadonlyKeyBasedMapOperation implements BlockingOperation {

    private EntryView<Data, Data> result;

    public GetEntryViewOperation() {
    }

    public GetEntryViewOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    protected void runInternal() {
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record != null) {
            Data value = mapServiceContext.toData(record.getValue());
            ExpiryMetadata expiryMetadata = recordStore.getExpirySystem().getExpiryMetadata(dataKey);
            result = EntryViews.createSimpleEntryView(dataKey, value, record, expiryMetadata);
        }
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    public boolean shouldWait() {
        return recordStore.isTransactionallyLocked(dataKey)
                && !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.GET_ENTRY_VIEW;
    }
}
