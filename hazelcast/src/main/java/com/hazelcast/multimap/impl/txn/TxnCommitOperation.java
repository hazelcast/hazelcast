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

import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.AbstractBackupAwareMultiMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.impl.operationexecutor.OperationRunner.runDirect;

public class TxnCommitOperation extends AbstractBackupAwareMultiMapOperation implements Notifier {

    private List<Operation> opList;

    private transient boolean notify = true;

    public TxnCommitOperation() {
    }

    public TxnCommitOperation(int partitionId, String name, Data dataKey, long threadId, List<Operation> opList) {
        super(name, dataKey, threadId);
        setPartitionId(partitionId);
        this.opList = opList;
    }

    @Override
    public void run() throws Exception {
        for (Operation op : opList) {
            op.setNodeEngine(getNodeEngine())
                    .setServiceName(getServiceName())
                    .setPartitionId(getPartitionId());
            runDirect(op);
        }
        getOrCreateContainer().unlock(dataKey, getCallerUuid(), threadId, getCallId());
    }

    @Override
    public boolean shouldBackup() {
        return notify;
    }

    @Override
    public Operation getBackupOperation() {
        List<Operation> backupOpList = new ArrayList<Operation>();
        for (Operation operation : opList) {
            if (operation instanceof BackupAwareOperation) {
                BackupAwareOperation backupAwareOperation = (BackupAwareOperation) operation;
                if (backupAwareOperation.shouldBackup()) {
                    backupOpList.add(backupAwareOperation.getBackupOperation());
                }
            }
        }
        return new TxnCommitBackupOperation(name, dataKey, backupOpList, getCallerUuid(), threadId);
    }

    @Override
    public boolean shouldNotify() {
        return notify;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_COMMIT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(opList.size());
        for (Operation op : opList) {
            out.writeObject(op);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        opList = new ArrayList<Operation>(size);
        for (int i = 0; i < size; i++) {
            opList.add((Operation) in.readObject());
        }
    }
}
