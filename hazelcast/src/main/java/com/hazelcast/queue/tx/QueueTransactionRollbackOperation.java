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
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.queue.QueueOperation;

import java.io.IOException;

/**
 * Transaction Rollback Operation for the Queue.
 */
public class QueueTransactionRollbackOperation extends QueueOperation {

    private String transactionId;

    public QueueTransactionRollbackOperation() {
    }

    public QueueTransactionRollbackOperation(String name, String transactionId) {
        super(name);
        this.transactionId = transactionId;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TRANSACTION_ROLLBACK;
    }

    @Override
    public void run() throws Exception {
        QueueContainer container = getOrCreateContainer();
        container.rollbackTransaction(transactionId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
