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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.MultiMapBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;
import java.io.IOException;

public class TxnPrepareOperation extends MultiMapBackupAwareOperation {

    private static final long LOCK_EXTENSION_TIME_IN_MILLIS = 10000L;
    long ttl;

    public TxnPrepareOperation() {
    }

    public TxnPrepareOperation(String name, Data dataKey, long ttl, long threadId) {
        super(name, dataKey, threadId);
        this.ttl = ttl;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (!container.extendLock(dataKey, getCallerUuid(), threadId, LOCK_EXTENSION_TIME_IN_MILLIS)) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! -> " + container.getLockOwnerInfo(dataKey)
            );
        }
        response = true;
    }

    public boolean shouldBackup() {
        return true;
    }

    public boolean shouldWait() {
        return false;
    }

    public Operation getBackupOperation() {
        return new TxnPrepareBackupOperation(name, dataKey, getCallerUuid(), threadId);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(ttl);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ttl = in.readLong();
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_PREPARE;
    }
}
