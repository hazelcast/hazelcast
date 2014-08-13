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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

public class TxnRollbackOperation extends MultiMapBackupAwareOperation implements Notifier {

    public TxnRollbackOperation() {
    }

    public TxnRollbackOperation(String name, Data dataKey, long threadId) {
        super(name, dataKey, threadId);
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (container.isLocked(dataKey) && !container.unlock(dataKey, getCallerUuid(), threadId)) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! Owner: " + container.getLockOwnerInfo(dataKey)
            );
        }
    }

    public Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(name, dataKey, getCallerUuid(), threadId);
    }

    public boolean shouldNotify() {
        return true;
    }

    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_ROLLBACK;
    }
}
