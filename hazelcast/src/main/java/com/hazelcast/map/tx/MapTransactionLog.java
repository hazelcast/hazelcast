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

import com.hazelcast.transaction.KeyAwareTransactionLog;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionLog;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Future;

public class MapTransactionLog implements KeyAwareTransactionLog {

    // todo remove version as it is already defined in operation
    long version;
    String name;
    Data key;
    int threadId = ThreadUtil.getThreadId();
    Operation op;

    public MapTransactionLog() {
    }

    public MapTransactionLog(String name, Data key, Operation op, long version) {
        this.name = name;
        this.key = key;
        this.version = version;
        if (!(op instanceof MapTxnOperation)) {
            throw new IllegalArgumentException();
        }
        this.op = op;
    }

    @Override
    public Future prepare(NodeEngine nodeEngine) throws TransactionException {
        TxnPrepareOperation operation = new TxnPrepareOperation(name, key);
        operation.setThreadId(threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public Future commit(NodeEngine nodeEngine) {
        MapTxnOperation txnOp = (MapTxnOperation) op;
        txnOp.setThreadId(threadId);
        txnOp.setVersion(version);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, op, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future rollback(NodeEngine nodeEngine) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        TxnRollbackOperation operation = new TxnRollbackOperation(name, key);
        operation.setThreadId(threadId);
        try {
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(version);
        boolean isNullKey = key == null;
        out.writeBoolean(isNullKey);
        if (!isNullKey) {
            key.writeData(out);
        }
        out.writeInt(threadId);
        out.writeObject(op);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        version = in.readLong();
        boolean isNullKey = in.readBoolean();
        if (!isNullKey) {
            key = new Data();
            key.readData(in);
        }
        threadId = in.readInt();
        op = in.readObject();
    }

    @Override
    public Object getKey() {
        return key;
    }
}
