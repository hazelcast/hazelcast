/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.impl.operations.ChangeClusterStateOperation;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOperation;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOperation;
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

    private ClusterState newState;
    private Address initiator;
    private Address target;
    private String txnId;
    private long leaseTime;
    private int partitionStateVersion;

    public ClusterStateTransactionLogRecord() {
    }

    public ClusterStateTransactionLogRecord(ClusterState newState, Address initiator, Address target,
            String txnId, long leaseTime, int partitionStateVersion) {
        Preconditions.checkNotNull(newState);
        Preconditions.checkNotNull(initiator);
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(txnId);
        Preconditions.checkPositive(leaseTime, "Lease time should be positive!");

        this.newState = newState;
        this.initiator = initiator;
        this.target = target;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
        this.partitionStateVersion = partitionStateVersion;
    }

    @Override
    public Object getKey() {
        return null;
    }

    @Override
    public Operation newPrepareOperation() {
        return new LockClusterStateOperation(newState, initiator, txnId, leaseTime, partitionStateVersion);
    }

    @Override
    public Operation newCommitOperation() {
        return new ChangeClusterStateOperation(newState, initiator, txnId);
    }

    @Override
    public Operation newRollbackOperation() {
        return new RollbackClusterStateOperation(initiator, txnId);
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(newState.toString());
        initiator.writeData(out);
        target.writeData(out);
        out.writeUTF(txnId);
        out.writeLong(leaseTime);
        out.writeInt(partitionStateVersion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        String stateName = in.readUTF();
        newState = ClusterState.valueOf(stateName);
        initiator = new Address();
        initiator.readData(in);
        target = new Address();
        target.readData(in);
        txnId = in.readUTF();
        leaseTime = in.readLong();
        partitionStateVersion = in.readInt();
    }
}
