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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

import static com.hazelcast.multimap.impl.txn.TxnPrepareOperation.LOCK_EXTENSION_TIME_IN_MILLIS;

public class TxnPrepareBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation, Versioned {

    private String caller;

    public TxnPrepareBackupOperation() {
    }

    public TxnPrepareBackupOperation(String name, Data dataKey, long threadId, String caller) {
        super(name, dataKey, threadId);
        this.caller = caller;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        if (!container.txnLock(dataKey, caller, threadId, getCallId(), LOCK_EXTENSION_TIME_IN_MILLIS, true)) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! -> " + container.getLockOwnerInfo(dataKey)
            );
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(caller);
        // RU_COMPAT_3_9
        if (out.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            out.writeLong(0);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        caller = in.readUTF();
        // RU_COMPAT_3_9
        if (in.getVersion().isUnknownOrLessThan(Versions.V3_10)) {
            in.readLong();
        }
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.TXN_PREPARE_BACKUP;
    }
}
