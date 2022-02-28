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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.AbstractBackupAwareMultiMapOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

public class TxnRollbackOperation extends AbstractBackupAwareMultiMapOperation implements Notifier {

    public TxnRollbackOperation() {
    }

    public TxnRollbackOperation(int partitionId, String name, Data dataKey, long threadId) {
        super(name, dataKey, threadId);
        setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (container.isLocked(dataKey) && !container.unlock(dataKey, getCallerUuid(), threadId, getCallId())) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! Owner: " + container.getLockOwnerInfo(dataKey)
            );
        }
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(name, dataKey, getCallerUuid(), threadId);
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_ROLLBACK;
    }
}
