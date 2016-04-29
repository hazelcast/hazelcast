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

public class ChangeClusterStateOperation extends AbstractOperation implements AllowedDuringPassiveState {

    private ClusterState newState;
    private Address initiator;
    private String txnId;
    private String stateName;

    public ChangeClusterStateOperation() {
    }

    public ChangeClusterStateOperation(ClusterState newState, Address initiator, String txnId) {
        this.newState = newState;
        this.initiator = initiator;
        this.txnId = txnId;
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
        getLogger().info("Changing cluster state state to " + newState + ", Initiator: " + initiator);
        clusterStateManager.commitClusterState(newState, initiator, txnId);
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
        out.writeUTF(newState.toString());
        initiator.writeData(out);
        out.writeUTF(txnId);
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
    }
}
