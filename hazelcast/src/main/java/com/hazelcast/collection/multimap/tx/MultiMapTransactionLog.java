/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionLog;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @ali 3/29/13
 */
public class MultiMapTransactionLog implements TransactionLog {

    CollectionProxyId proxyId;
    Operation op;
    Data key;
    long ttl;
    int threadId;

    public MultiMapTransactionLog() {
    }

    public MultiMapTransactionLog(Data key, CollectionProxyId proxyId, long ttl, int threadId, Operation op) {
        this.key = key;
        this.proxyId = proxyId;
        this.ttl = ttl;
        this.threadId = threadId;
        this.op = op;
    }

    public Future prepare(NodeEngine nodeEngine) {
        TxnPrepareOperation operation = new TxnPrepareOperation(proxyId, key, ttl, threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future commit(NodeEngine nodeEngine) {
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, op, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future rollback(NodeEngine nodeEngine) {
        TxnRollbackOperation operation = new TxnRollbackOperation(proxyId, key, threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        proxyId.writeData(out);
        out.writeObject(op);
        key.writeData(out);
        out.writeLong(ttl);
        out.writeInt(threadId);
    }

    public void readData(ObjectDataInput in) throws IOException {
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        op = in.readObject();
        key = new Data();
        key.readData(in);
        ttl = in.readLong();
        threadId = in.readInt();
    }
}
