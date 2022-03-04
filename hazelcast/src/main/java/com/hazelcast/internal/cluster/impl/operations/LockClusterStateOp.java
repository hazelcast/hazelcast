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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.ClusterStateChange;
import com.hazelcast.internal.cluster.impl.ClusterStateManager;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

public class LockClusterStateOp  extends Operation implements AllowedDuringPassiveState, UrgentSystemOperation,
        IdentifiedDataSerializable, Versioned {

    private ClusterStateChange stateChange;
    private Address initiator;
    private UUID txnId;
    private long leaseTime;
    private int memberListVersion;
    private long partitionStateStamp;

    public LockClusterStateOp() {
    }

    public LockClusterStateOp(ClusterStateChange stateChange, Address initiator, UUID txnId, long leaseTime,
            int memberListVersion, long partitionStateStamp) {
        this.stateChange = stateChange;
        this.initiator = initiator;
        this.txnId = txnId;
        this.leaseTime = leaseTime;
        this.memberListVersion = memberListVersion;
        this.partitionStateStamp = partitionStateStamp;
    }

    @Override
    public void beforeRun() throws Exception {
        if (stateChange == null) {
            throw new IllegalArgumentException("Invalid null cluster state");
        }
        stateChange.validate();
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
        clusterStateManager.lockClusterState(stateChange, initiator, txnId, leaseTime, memberListVersion, partitionStateStamp);
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof TransactionException || e instanceof IllegalStateException) {
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
        out.writeObject(stateChange);
        out.writeObject(initiator);
        UUIDSerializationUtil.writeUUID(out, txnId);
        out.writeLong(leaseTime);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeLong(partitionStateStamp);
        } else {
            out.writeInt((int) partitionStateStamp);
        }
        out.writeInt(memberListVersion);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        stateChange = in.readObject();
        initiator = in.readObject();
        txnId = UUIDSerializationUtil.readUUID(in);
        leaseTime = in.readLong();
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            partitionStateStamp = in.readLong();
        } else {
            partitionStateStamp = in.readInt();
        }
        memberListVersion = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.LOCK_CLUSTER_STATE;
    }

}
