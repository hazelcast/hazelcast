/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.spi.impl.operationexecutor.OperationRunner.runDirect;

public class TxnCommitBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    private List<Operation> opList;
    private UUID caller;

    public TxnCommitBackupOperation() {
    }

    public TxnCommitBackupOperation(String name, Data dataKey, List<Operation> opList, UUID caller, long threadId) {
        super(name, dataKey);
        this.opList = opList;
        this.caller = caller;
        this.threadId = threadId;
    }

    @Override
    public void run() throws Exception {
        for (Operation op : opList) {
            op.setNodeEngine(getNodeEngine()).setServiceName(getServiceName()).setPartitionId(getPartitionId());
            runDirect(op);
        }
        // changed to forceUnlock because replica-sync of lock causes problems, same as IMap
        // real solution is to make 'lock-and-get' backup-aware
        getOrCreateContainerWithoutAccess().forceUnlock(dataKey);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeList(opList, out);
        UUIDSerializationUtil.writeUUID(out, caller);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        opList = SerializationUtil.readList(in);
        caller = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_COMMIT_BACKUP;
    }
}
