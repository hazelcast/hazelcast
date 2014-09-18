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

package com.hazelcast.collection.txn;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.KeyAwareTransactionLog;
import com.hazelcast.util.ExceptionUtil;
import java.io.IOException;
import java.util.concurrent.Future;

public class CollectionTransactionLog implements KeyAwareTransactionLog {

    String transactionId;
    private long itemId;
    private String name;
    private Operation op;
    private int partitionId;
    private String serviceName;

    public CollectionTransactionLog() {
    }

    public CollectionTransactionLog(long itemId,
                                    String name,
                                    int partitionId,
                                    String serviceName,
                                    String transactionId,
                                    Operation op) {
        this.itemId = itemId;
        this.name = name;
        this.op = op;
        this.partitionId = partitionId;
        this.serviceName = serviceName;
        this.transactionId = transactionId;
    }

    @Override
    public Object getKey() {
        return new TransactionLogKey(name, itemId, serviceName);
    }

    @Override
    public Future prepare(NodeEngine nodeEngine) {
        boolean removeOperation = op instanceof CollectionTxnRemoveOperation;
        CollectionPrepareOperation operation = new CollectionPrepareOperation(name, itemId, transactionId, removeOperation);
        try {
            return nodeEngine.getOperationService().invokeOnPartition(serviceName, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public Future commit(NodeEngine nodeEngine) {
        try {
            return nodeEngine.getOperationService().invokeOnPartition(serviceName, op, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public Future rollback(NodeEngine nodeEngine) {
        boolean removeOperation = op instanceof CollectionTxnRemoveOperation;
        CollectionRollbackOperation operation = new CollectionRollbackOperation(name, itemId, removeOperation);
        try {
            return nodeEngine.getOperationService().invokeOnPartition(serviceName, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeUTF(serviceName);
        out.writeObject(op);
        out.writeUTF(transactionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        name = in.readUTF();
        partitionId = in.readInt();
        serviceName = in.readUTF();
        op = in.readObject();
        transactionId = in.readUTF();
    }


}
