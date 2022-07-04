/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.operations.CommitClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.LockClusterStateOp;
import com.hazelcast.internal.cluster.impl.operations.RollbackClusterStateOp;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.impl.TargetAwareTransactionLogRecord;

import java.io.IOException;
import java.util.UUID;

/**
 * TransactionLogRecord implementation to be used in {@code ClusterState} transactions.
 *
 * @see ClusterState
 * @see Cluster#changeClusterState(ClusterState, com.hazelcast.transaction.TransactionOptions)
 */
public class ClusterStateTransactionLogRecord implements TargetAwareTransactionLogRecord, Versioned {

    ClusterStateChange stateChange;
    Address initiator;
    Address target;
    UUID txnId;
    long leaseTime;
    int memberListVersion;
    long partitionStateStamp;
    boolean isTransient;

    public ClusterStateTransactionLogRecord() {
    }

    public ClusterStateTransactionLogRecord(ClusterStateChange stateChange, Address initiator, Address target,
                                            UUID txnId, long leaseTime, int memberListVersion,
                                            long partitionStateStamp, boolean isTransient) {
        this.memberListVersion = memberListVersion;
        Preconditions.checkNotNull(stateChange);
        Preconditions.checkNotNull(initiator);
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(txnId);
        Preconditions.checkPositive("leaseTime", leaseTime);

        this.stateChange = stateChange;
        this.initiator = initiator;
        this.target = target;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
        this.partitionStateStamp = partitionStateStamp;
        this.isTransient = isTransient;
    }

    @Override
    public Object getKey() {
        return null;
    }

    @Override
    public Operation newPrepareOperation() {
        return new LockClusterStateOp(stateChange, initiator, txnId, leaseTime, memberListVersion, partitionStateStamp);
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
        UUIDSerializationUtil.writeUUID(out, txnId);
        out.writeLong(leaseTime);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeLong(partitionStateStamp);
        } else {
            out.writeInt((int) partitionStateStamp);
        }
        out.writeBoolean(isTransient);
        out.writeInt(memberListVersion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        stateChange = in.readObject();
        initiator = in.readObject();
        target = in.readObject();
        txnId = UUIDSerializationUtil.readUUID(in);
        leaseTime = in.readLong();
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            partitionStateStamp = in.readLong();
        } else {
            partitionStateStamp = in.readInt();
        }
        isTransient = in.readBoolean();
        memberListVersion = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.CLUSTER_STATE_TRANSACTION_LOG_RECORD;
    }
}
