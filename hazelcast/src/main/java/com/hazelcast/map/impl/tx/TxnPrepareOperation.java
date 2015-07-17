/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.tx;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;
import java.io.IOException;

/**
 * An operation to prepare transaction by locking the key on the key owner.
 */
public class TxnPrepareOperation extends KeyBasedMapOperation implements BackupAwareOperation, MutatingOperation {

    private static final long LOCK_TTL_MILLIS = 10000L;

    private String ownerUuid;

    protected TxnPrepareOperation(int partitionId, String name, Data dataKey, String ownerUuid) {
        super(name, dataKey);
        setPartitionId(partitionId);
        this.ownerUuid = ownerUuid;
    }

    public TxnPrepareOperation() {
    }

    @Override
    public void run() throws Exception {
        if (!recordStore.extendLock(getKey(), ownerUuid, getThreadId(), LOCK_TTL_MILLIS)) {
            ILogger logger = getLogger();
            logger.severe("Locked: [" + recordStore.isLocked(getKey()) + "], key: [" + getKey() + ']');
            throw new TransactionException("Lock is not owned by the transaction! ["
                    + recordStore.getLockOwnerInfo(getKey()) + ']');
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public final Operation getBackupOperation() {
        return new TxnPrepareBackupOperation(name, dataKey, ownerUuid, getThreadId());
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(ownerUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ownerUuid = in.readUTF();
    }

    @Override
    public String toString() {
        return "TxnPrepareOperation{"
                + "ownerUuid='" + ownerUuid + '\''
                + '}';
    }
}
