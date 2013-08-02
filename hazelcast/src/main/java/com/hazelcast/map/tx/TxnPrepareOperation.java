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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;

public class TxnPrepareOperation extends KeyBasedMapOperation implements BackupAwareOperation {

    protected TxnPrepareOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public TxnPrepareOperation() {
    }

    @Override
    public void run() throws Exception {
        if (!recordStore.extendLock(getKey(), getCallerUuid(), getThreadId(), 10000L)) {
            throw new TransactionException("Lock is not owned by the transaction! Owner: " + recordStore.getLockOwnerInfo(getKey()));
        }
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    public boolean shouldBackup() {
        return true;
    }

    public final Operation getBackupOperation() {
        return new TxnPrepareBackupOperation(name, dataKey, getCallerUuid(), getThreadId());
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }
}
