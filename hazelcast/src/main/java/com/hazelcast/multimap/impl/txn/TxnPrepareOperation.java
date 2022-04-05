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
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.TransactionException;

import java.util.concurrent.TimeUnit;

public class TxnPrepareOperation extends AbstractBackupAwareMultiMapOperation {

    static final long LOCK_EXTENSION_TIME_IN_MILLIS = TimeUnit.SECONDS.toMillis(10);

    public TxnPrepareOperation() {
    }

    public TxnPrepareOperation(int partitionId, String name, Data dataKey, long threadId) {
        super(name, dataKey, threadId);

        setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (!container.extendLock(dataKey, getCallerUuid(), threadId, LOCK_EXTENSION_TIME_IN_MILLIS)) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! -> " + container.getLockOwnerInfo(dataKey)
            );
        }
        response = true;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public boolean shouldWait() {
        return false;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnPrepareBackupOperation(name, dataKey, threadId, getCallerUuid());
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_PREPARE;
    }
}
