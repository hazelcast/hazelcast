/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftNodeAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * The operation that passes a query to leader or a follower of a Raft group.
 * The given query can run locally on leader or a follower, or can be committed
 * to the Raft group, depending on query policy.
 * <p/>
 * Please note that the given query can be committed twice if the leader
 * commits the query but fails before sending the response, therefore the query
 * operation is expected to have no side-effect.
 */
public class RaftQueryOp extends Operation implements IndeterminateOperationStateAware, RaftSystemOperation, ExecutionCallback,
                                                      IdentifiedDataSerializable {

    private CPGroupId groupId;
    private QueryPolicy queryPolicy;
    private Object op;

    public RaftQueryOp() {
    }

    public RaftQueryOp(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        this.groupId = groupId;
        this.op = raftOp;
        this.queryPolicy = queryPolicy;
    }

    @Override
    public final void run() {
        RaftService service = getService();
        RaftNode raftNode = service.getRaftNode(groupId);
        if (raftNode == null) {
            if (service.isRaftGroupDestroyed(groupId)) {
                sendResponse(new CPGroupDestroyedException(groupId));
            } else {
                sendResponse(new NotLeaderException(groupId, service.getLocalCPMember(), null));
            }
            return;
        } else if (raftNode.getStatus() == RaftNodeStatus.STEPPED_DOWN) {
            service.stepDownRaftNode(groupId);
            sendResponse(new NotLeaderException(groupId, service.getLocalCPMember(), null));
            return;
        }

        if (op instanceof RaftNodeAware) {
            ((RaftNodeAware) op).setRaftNode(raftNode);
        }

        ICompletableFuture future = raftNode.query(op, queryPolicy);
        future.andThen(this);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        sendResponse(t);
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_QUERY_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeObject(op);
        out.writeUTF(queryPolicy.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        op = in.readObject();
        queryPolicy = QueryPolicy.valueOf(in.readUTF());
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", op=").append(op)
          .append(", groupId=").append(groupId)
          .append(", policy=").append(queryPolicy);
    }

}
