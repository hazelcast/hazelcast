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

package com.hazelcast.cp.internal.operation.unsafe;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CallerAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Base class for Raft operations to be executed in UNSAFE mode.
 * {@code AbstractUnsafeRaftOp} operation wraps original {@link RaftOp}
 * and executes it outside of Raft algorithm.
 */
public abstract class AbstractUnsafeRaftOp extends Operation implements IdentifiedDataSerializable {

    CPGroupId groupId;
    RaftOp op;

    private transient Object response;

    AbstractUnsafeRaftOp() {
    }

    AbstractUnsafeRaftOp(CPGroupId groupId, RaftOp op) {
        this.groupId = groupId;
        this.op = op;
    }

    @Override
    public final CallStatus call() throws Exception {
        RaftService service = getService();
        if (service.isCpSubsystemEnabled()) {
            throw new IllegalStateException("CP subsystem is enabled on this member, "
                    + "but received an UNSAFE operation! This could be due to a misconfiguration on the caller side. "
                    + "CP subsystem configuration must be the same on all members.");
        }

        NodeEngine nodeEngine = getNodeEngine();
        if (op instanceof CallerAware) {
            ((CallerAware) op).setCaller(getCallerAddress(), getCallId());
        }
        long commitIndex = service.nextUnsafeModeCommitIndex(groupId);
        response = op.setNodeEngine(nodeEngine).run(groupId, commitIndex);
        return handleResponse(commitIndex, response);
    }

    abstract CallStatus handleResponse(long commitIndex, Object response);

    public final CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeObject(op);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        op = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", groupId=").append(groupId);
        sb.append(", op=").append(op);
    }
}
