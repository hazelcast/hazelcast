/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnqueue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TxnRetainAllBackupOperation extends QueueOperation implements BackupOperation {
    private List<Data> data;

    public TxnRetainAllBackupOperation() {
    }

    public TxnRetainAllBackupOperation(String name, List<Data> data) {
        super(name);
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        response = !queueContainer.txnCommitRetainAll(data).isEmpty();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_RETAIN_ALL_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(data.size());
        for (Data datum : data) {
            IOUtil.writeData(out, datum);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(IOUtil.readData(in));
        }
    }
}
