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

package com.hazelcast.queue.tx;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.transaction.impl.KeyAwareTransactionLog;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * This class contains Transaction log for the Queue.
 */
public class QueueTransactionLog implements KeyAwareTransactionLog {

    private long itemId;
    private String name;
    private Operation op;
    private int partitionId;
    private String transactionId;

    public QueueTransactionLog() {
    }

    public QueueTransactionLog(String transactionId, long itemId, String name, int partitionId, Operation op) {
        this.transactionId = transactionId;
        this.itemId = itemId;
        this.name = name;
        this.partitionId = partitionId;
        this.op = op;
    }

    @Override
    public Future prepare(NodeEngine nodeEngine) {
        boolean pollOperation = op instanceof TxnPollOperation;
        TxnPrepareOperation operation = new TxnPrepareOperation(name, itemId, pollOperation, transactionId);
        try {
            return invoke(nodeEngine, operation);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private InternalCompletableFuture invoke(NodeEngine nodeEngine, Operation operation) {
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.invokeOnPartition(QueueService.SERVICE_NAME, operation, partitionId);
    }

    @Override
    public Future commit(NodeEngine nodeEngine) {
        try {
            OperationService operationService = nodeEngine.getOperationService();
            return operationService.invokeOnPartition(QueueService.SERVICE_NAME, op, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public Future rollback(NodeEngine nodeEngine) {
        boolean pollOperation = op instanceof TxnPollOperation;
        TxnRollbackOperation operation = new TxnRollbackOperation(name, itemId, pollOperation);
        try {
            return invoke(nodeEngine, operation);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(transactionId);
        out.writeLong(itemId);
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeObject(op);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        transactionId = in.readUTF();
        itemId = in.readLong();
        name = in.readUTF();
        partitionId = in.readInt();
        op = in.readObject();
    }

    @Override
    public Object getKey() {
        return new TransactionLogKey(itemId, name);
    }
}
