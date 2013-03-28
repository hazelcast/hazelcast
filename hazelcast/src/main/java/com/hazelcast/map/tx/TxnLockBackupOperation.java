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

import com.hazelcast.concurrent.lock.LockBackupOperation;
import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.map.LockAwareOperation;
import com.hazelcast.map.MapService;
import com.hazelcast.map.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

public class TxnLockBackupOperation extends LockAwareOperation implements BackupOperation {

    private long timeout;
    private String callerId;
    private int threadId;

    public TxnLockBackupOperation() {
    }

    public TxnLockBackupOperation(String name, Data dataKey, long timeout, String callerId, int threadId) {
        super(name, dataKey, -1);
        this.timeout = timeout;
        this.callerId = callerId;
        this.threadId = threadId;
    }

    @Override
    public void run() throws Exception {
        if (!recordStore.lock(getKey(), callerId, threadId, ttl)) {
            throw new TransactionException("Lock failed.");
        }
    }

    @Override
    public long getWaitTimeoutMillis() {
        return timeout;
    }

    @Override
    public void onWaitExpire() {
        final ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(null);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(timeout);
        out.writeInt(threadId);
        out.writeUTF(callerId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        timeout = in.readLong();
        threadId = in.readInt();
        callerId = in.readUTF();
    }


    @Override
    public String toString() {
        return "TxnLockBackupOperation{" +
                "timeout=" + timeout +
                ", thread=" + getThreadId() +
                '}';
    }

}
