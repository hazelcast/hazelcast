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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.ClusterStateManager;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;

public class LockClusterStateOperation extends AbstractOperation implements AllowedDuringPassiveState {

    private String stateName;
    private ClusterState newState;
    private Address initiator;
    private String txnId;
    private long leaseTime;
    private int partitionStateVersion;

    public LockClusterStateOperation() {
    }

    public LockClusterStateOperation(ClusterState newState, Address initiator, String txnId,
                                     long leaseTime, int partitionStateVersion) {
        this.newState = newState;
        this.initiator = initiator;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
        this.partitionStateVersion = partitionStateVersion;
    }

    @Override
    public void beforeRun() throws Exception {
        if (newState == null) {
            throw new IllegalArgumentException("Unknown cluster state: " + stateName);
        }
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        ClusterStateManager clusterStateManager = service.getClusterStateManager();
        ClusterState state = clusterStateManager.getState();
        if (state == ClusterState.IN_TRANSITION) {
            getLogger().info("Extending cluster state lock. Initiator: " + initiator
                    + ", lease-time: " + leaseTime);
        } else {
            getLogger().info("Locking cluster state. Initiator: " + initiator
                    + ", lease-time: " + leaseTime);
        }
        clusterStateManager.lockClusterState(newState, initiator, txnId, leaseTime, partitionStateVersion);
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof TransactionException) {
            getLogger().severe(e.getMessage());
        } else {
            super.logError(e);
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(newState.toString());
        initiator.writeData(out);
        out.writeUTF(txnId);
        out.writeLong(leaseTime);
        out.writeInt(partitionStateVersion);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        stateName = in.readUTF();
        try {
            newState = ClusterState.valueOf(stateName);
        } catch (IllegalArgumentException ignored) {
            EmptyStatement.ignore(ignored);
        }
        initiator = new Address();
        initiator.readData(in);
        txnId = in.readUTF();
        leaseTime = in.readLong();
        partitionStateVersion = in.readInt();
    }
}
