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
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.queue.QueueOperation;

import java.io.IOException;

/**
 * @ali 9/5/13
 */
public class QueueTransactionRollbackOperation extends QueueOperation {

    String transactionId;

    public QueueTransactionRollbackOperation() {
    }

    public QueueTransactionRollbackOperation(String name, String transactionId) {
        super(name);
        this.transactionId = transactionId;
    }

    public int getId() {
        return QueueDataSerializerHook.CHECK_EVICT;
    }

    public void run() throws Exception {
        getOrCreateContainer().rollbackTransaction(transactionId);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
    }

    public boolean returnsResponse() {
        return false;
    }
}
