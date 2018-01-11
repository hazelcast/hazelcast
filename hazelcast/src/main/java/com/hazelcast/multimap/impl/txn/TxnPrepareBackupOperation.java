/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

public class TxnPrepareBackupOperation extends MultiMapKeyBasedOperation implements BackupOperation {

    private static final long LOCK_EXTENSION_TIME_IN_MILLIS = 10000L;
    String caller;
    long ttl;

    public TxnPrepareBackupOperation() {
    }

    public TxnPrepareBackupOperation(String name, Data dataKey, String caller, long threadId) {
        super(name, dataKey);
        this.caller = caller;
        this.threadId = threadId;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (!container.txnLock(dataKey, caller, threadId, getCallId(), ttl + LOCK_EXTENSION_TIME_IN_MILLIS, true)) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! -> " + container.getLockOwnerInfo(dataKey)
            );
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(caller);
        out.writeLong(ttl);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        caller = in.readUTF();
        ttl = in.readLong();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.TXN_PREPARE_BACKUP;
    }
}
