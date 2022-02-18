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

package com.hazelcast.cp.internal.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CallerAware;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.io.IOException;

/**
 * The operation used by {@link RaftInvocationManager} to replicate a given
 * {@link RaftOp} to leader of the target Raft group. The leader sends
 * the response for this operation after it commits the given operation
 * to the majority of the Raft group.
 */
public class DefaultRaftReplicateOp extends RaftReplicateOp implements IndeterminateOperationStateAware {

    private RaftOp op;

    public DefaultRaftReplicateOp() {
    }

    public DefaultRaftReplicateOp(CPGroupId groupId, RaftOp op) {
        super(groupId);
        this.op = op;
    }

    @Override
    protected InternalCompletableFuture replicate(RaftNode raftNode) {
        if (op instanceof CallerAware) {
            ((CallerAware) op).setCaller(getCallerAddress(), getCallId());
        }

        return raftNode.replicate(op);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        if (op instanceof IndeterminateOperationStateAware) {
            return ((IndeterminateOperationStateAware) op).isRetryableOnIndeterminateOperationState();
        }

        return false;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(op);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        op = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", op=").append(op);
    }
}
