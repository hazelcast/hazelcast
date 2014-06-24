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

import com.hazelcast.map.operation.KeyBasedMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.transaction.TransactionException;
import java.io.IOException;

public class TxnPrepareBackupOperation extends KeyBasedMapOperation implements BackupOperation {

    private String lockOwner;
    private long lockThreadId;

    protected TxnPrepareBackupOperation(String name, Data dataKey, String lockOwner, long lockThreadId) {
        super(name, dataKey);
        this.lockOwner = lockOwner;
        this.lockThreadId = lockThreadId;
    }

    public TxnPrepareBackupOperation() {
    }

    @Override
    public void run() throws Exception {
        if (!recordStore.txnLock(getKey(), lockOwner, lockThreadId, 10000L)) {
            throw new TransactionException("Lock is not owned by the transaction! Caller: " + lockOwner + ", Owner: " + recordStore.getLockOwnerInfo(getKey()));
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(lockOwner);
        out.writeLong(lockThreadId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        lockOwner = in.readUTF();
        lockThreadId = in.readLong();
    }
}
