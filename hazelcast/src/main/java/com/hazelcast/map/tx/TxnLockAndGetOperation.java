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

package com.hazelcast.map.tx;

import com.hazelcast.map.operation.LockAwareOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.transaction.TransactionException;
import java.io.IOException;

/**
 * Transactional lock and get operation.
 */
public class TxnLockAndGetOperation extends LockAwareOperation {

    private VersionedValue response;
    private String ownerUuid;

    public TxnLockAndGetOperation() {
    }

    public TxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, String ownerUuid) {
        super(name, dataKey, ttl);
        this.ownerUuid = ownerUuid;
        setWaitTimeout(timeout);
    }

    @Override
    public void run() throws Exception {
        if (!recordStore.txnLock(getKey(), ownerUuid, getThreadId(), ttl)) {
            throw new TransactionException("Transaction couldn't obtain lock.");
        }
        Record record = recordStore.getRecord(dataKey);
        Data value = record == null ? null : mapService.toData(record.getValue());
        response = new VersionedValue(value, record == null ? 0 : record.getVersion());
    }

    public boolean shouldWait() {
        return !recordStore.canAcquireLock(dataKey, ownerUuid, getThreadId());
    }

    @Override
    public void onWaitExpire() {
        final ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(null);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = in.readUTF();
    }


    @Override
    public String toString() {
        return "TxnLockAndGetOperation{"
                + "timeout=" + getWaitTimeout()
                + ", thread=" + getThreadId()
                + '}';
    }

}
