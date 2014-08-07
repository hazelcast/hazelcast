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

import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TxnCommitBackupOperation extends MultiMapKeyBasedOperation implements BackupOperation {

    List<Operation> opList;
    String caller;
    long threadId;

    public TxnCommitBackupOperation() {
    }

    public TxnCommitBackupOperation(String name, Data dataKey, List<Operation> opList, String caller, long threadId) {
        super(name, dataKey);
        this.opList = opList;
        this.caller = caller;
        this.threadId = threadId;
    }

    public void run() throws Exception {
        for (Operation op : opList) {
            op.setNodeEngine(getNodeEngine()).setServiceName(getServiceName()).setPartitionId(getPartitionId());
            op.beforeRun();
            op.run();
            op.afterRun();
        }
        getOrCreateContainer().unlock(dataKey, caller, threadId);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(opList.size());
        for (Operation op : opList) {
            out.writeObject(op);
        }
        out.writeUTF(caller);
        out.writeLong(threadId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        opList = new ArrayList<Operation>(size);
        for (int i = 0; i < size; i++) {
            opList.add((Operation) in.readObject());
        }
        caller = in.readUTF();
        threadId = in.readLong();
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_COMMIT_BACKUP;
    }
}
