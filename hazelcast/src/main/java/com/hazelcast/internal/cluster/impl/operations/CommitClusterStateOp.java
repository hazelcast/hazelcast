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

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.ClusterStateChange;
import com.hazelcast.internal.cluster.impl.ClusterStateManager;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

import static java.lang.String.format;

public class CommitClusterStateOp extends Operation implements AllowedDuringPassiveState, UrgentSystemOperation,
        IdentifiedDataSerializable {

    private ClusterStateChange stateChange;
    private Address initiator;
    private UUID txnId;
    private boolean isTransient;

    public CommitClusterStateOp() {
    }

    public CommitClusterStateOp(ClusterStateChange stateChange, Address initiator, UUID txnId, boolean isTransient) {
        this.stateChange = stateChange;
        this.initiator = initiator;
        this.txnId = txnId;
        this.isTransient = isTransient;
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
        getLogger().info(format("Changing cluster state from %s to %s, initiator: %s, transient: %s",
                clusterStateManager.stateToString(), stateChange, initiator, isTransient));
        clusterStateManager.commitClusterState(stateChange, initiator, txnId, isTransient);
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
    public Object getResponse() {
        return Boolean.TRUE;
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
        out.writeBoolean(isTransient);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        stateChange = in.readObject();
        initiator = in.readObject();
        txnId = UUIDSerializationUtil.readUUID(in);
        isTransient = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.CHANGE_CLUSTER_STATE;
    }
}
