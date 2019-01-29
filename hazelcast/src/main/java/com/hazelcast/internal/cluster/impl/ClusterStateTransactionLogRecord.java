/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.impl.operations.CommitClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOp;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.TargetAwareTransactionLogRecord;
import com.hazelcast.util.Preconditions;

import java.io.IOException;

/**
 * TransactionLogRecord implementation to be used in {@code ClusterState} transactions.
 *
 * @see ClusterState
 * @see com.hazelcast.core.Cluster#changeClusterState(ClusterState, com.hazelcast.transaction.TransactionOptions)
 */
public class ClusterStateTransactionLogRecord implements TargetAwareTransactionLogRecord {

    ClusterStateChange stateChange;
    Address initiator;
    Address target;
    String txnId;
    long leaseTime;
    int partitionStateVersion;
    boolean isTransient;

    public ClusterStateTransactionLogRecord() {
    }

    public ClusterStateTransactionLogRecord(ClusterStateChange stateChange, Address initiator, Address target,
            String txnId, long leaseTime, int partitionStateVersion, boolean isTransient) {
        Preconditions.checkNotNull(stateChange);
        Preconditions.checkNotNull(initiator);
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(txnId);
        Preconditions.checkPositive(leaseTime, "Lease time should be positive!");

        this.stateChange = stateChange;
        this.initiator = initiator;
        this.target = target;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
        this.partitionStateVersion = partitionStateVersion;
        this.isTransient = isTransient;
    }

    @Override
    public Object getKey() {
        return null;
    }

    @Override
    public Operation newPrepareOperation() {
        return new LockClusterStateOp(stateChange, initiator, txnId, leaseTime, partitionStateVersion);
    }

    @Override
    public Operation newCommitOperation() {
        return new CommitClusterStateOp(stateChange, initiator, txnId, isTransient);
    }

    @Override
    public Operation newRollbackOperation() {
        return new RollbackClusterStateOp(initiator, txnId);
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(stateChange);
        out.writeObject(initiator);
        out.writeObject(target);
        out.writeUTF(txnId);
        out.writeLong(leaseTime);
        out.writeInt(partitionStateVersion);
        out.writeBoolean(isTransient);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        stateChange = in.readObject();
        initiator = in.readObject();
        target = in.readObject();
        txnId = in.readUTF();
        leaseTime = in.readLong();
        partitionStateVersion = in.readInt();
        isTransient = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.CLUSTER_STATE_TRANSACTION_LOG_RECORD;
    }
}
