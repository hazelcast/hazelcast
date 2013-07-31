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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.operations.CollectionBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * @author ali 4/2/13
 */
public class TxnPrepareOperation extends CollectionBackupAwareOperation {

    long ttl;

    public TxnPrepareOperation() {
    }

    public TxnPrepareOperation(CollectionProxyId proxyId, Data dataKey, long ttl, int threadId) {
        super(proxyId, dataKey, threadId);
        this.ttl = ttl;
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        if (!container.extendLock(dataKey, getCallerUuid(), threadId, 10000L)){
            throw new TransactionException("Lock is not owned by the transaction! -> " + container.getLockOwnerInfo(dataKey));
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
        return new TxnPrepareBackupOperation(proxyId, dataKey, getCallerUuid(), threadId);
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
        return CollectionDataSerializerHook.TXN_PREPARE;
    }
}
