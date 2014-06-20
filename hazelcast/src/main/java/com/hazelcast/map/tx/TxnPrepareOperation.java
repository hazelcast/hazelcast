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

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.operation.KeyBasedMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;
import java.io.IOException;

public class TxnPrepareOperation extends KeyBasedMapOperation implements BackupAwareOperation {

    String ownerUuid;

    protected TxnPrepareOperation(String name, Data dataKey, String ownerUuid) {
        super(name, dataKey);
        this.ownerUuid = ownerUuid;
    }

    public TxnPrepareOperation() {
    }

    @Override
    public void run() throws Exception {
        if (!recordStore.extendLock(getKey(), ownerUuid, getThreadId(), 10000L)) {
            ILogger logger = getLogger();
            logger.severe(recordStore.isLocked(getKey()) + ":" + getKey());
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
        return new TxnPrepareBackupOperation(name, dataKey, ownerUuid, getThreadId());
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = in.readUTF();
    }
}
