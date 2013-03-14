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

package com.hazelcast.transaction;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @mdogan 3/4/13
 */
class TransactionLog implements DataSerializable {

    private String callerUuid;
    private String txnId;
    private int partitionId;
    private TransactionImpl.State state = Transaction.State.ACTIVE;
    private final List<TransactionalOperation> ops = new LinkedList<TransactionalOperation>();

    private transient boolean scheduled = false;
    private transient final AtomicBoolean control = new AtomicBoolean(false);

    TransactionLog() {
    }

    TransactionLog(String txnId, int partitionId) {
        this.txnId = txnId;
        this.partitionId = partitionId;
    }

    void addOperationRecord(TransactionalOperation operation) {
        // Normally we do not need sync block, since a tx can be executed only by a single thread.
        // But keeping sync block to prevent possible errors
        // if services send async (or response-less) transactional operations which can be executed concurrently.
        synchronized (this) {
            ops.add(operation);
        }
    }

    List<TransactionalOperation> getOperationRecords() {
        // Normally we do not need sync block, since a tx can be executed only by a single thread.
        // But keeping sync block to prevent possible errors
        // if services send async (or response-less) transactional operations which can be executed concurrently.
        synchronized (this) {
            return new ArrayList<TransactionalOperation>(ops);
        }
    }

    public String getCallerUuid() {
        return callerUuid;
    }

    public String getTxnId() {
        return txnId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public TransactionImpl.State getState() {
        return state;
    }

    public boolean isScheduled() {
        return scheduled;
    }

    void setScheduled(boolean scheduled) {
        this.scheduled = scheduled;
    }

    void setCallerUuid(String callerUuid) {
        this.callerUuid = callerUuid;
    }

    void setState(TransactionImpl.State state) {
        this.state = state;
    }

    boolean beginProcess() {
        return control.compareAndSet(false, true);
    }

    void endProcess() {
        control.set(false);
    }

    boolean isBeingProcessed() {
        return control.get();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(callerUuid);
        out.writeUTF(txnId);
        out.writeInt(partitionId);
        out.writeUTF(state.toString());
        int len = ops.size();
        out.writeInt(len);
        if (len > 0) {
            for (TransactionalOperation op : ops) {
                out.writeObject(op);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        callerUuid = in.readUTF();
        txnId = in.readUTF();
        partitionId = in.readInt();
        state = Transaction.State.valueOf(in.readUTF());
        int len = in.readInt();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                TransactionalOperation operation = in.readObject();
                operation.setState(state);
                addOperationRecord(operation);
            }
        }
    }
}
