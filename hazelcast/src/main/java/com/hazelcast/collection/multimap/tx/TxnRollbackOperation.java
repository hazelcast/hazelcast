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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

/**
 * @author ali 4/4/13
 */
public class TxnRollbackOperation extends CollectionBackupAwareOperation implements Notifier{

    public TxnRollbackOperation() {
    }

    public TxnRollbackOperation(CollectionProxyId proxyId, Data dataKey, int threadId) {
        super(proxyId, dataKey, threadId);
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        if (container.isLocked(dataKey) && !container.unlock(dataKey, getCallerUuid(), threadId)){
            throw new TransactionException("Lock is not owned by the transaction! Owner: " + container.getLockOwnerInfo(dataKey));
        }
    }

    public Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(proxyId, dataKey, getCallerUuid(), threadId);
    }

    public boolean shouldNotify() {
        return true;
    }

    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    public int getId() {
        return CollectionDataSerializerHook.TXN_ROLLBACK;
    }
}
