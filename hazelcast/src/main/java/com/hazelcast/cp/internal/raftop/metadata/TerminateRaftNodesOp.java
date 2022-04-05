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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * When a Raft group is destroyed, its members terminate their internal
 * {@link RaftNode} instances. This operation is sent to members of destroyed
 * Raft groups.
 * <p>
 * Please note that this operation is not a {@link RaftOp},
 * so it is not handled via the Raft layer.
 */
public class TerminateRaftNodesOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private Collection<CPGroupId> groupIds;

    public TerminateRaftNodesOp() {
    }

    public TerminateRaftNodesOp(Collection<CPGroupId> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    public void run() {
        RaftService service = getService();
        for (CPGroupId groupId : groupIds) {
            service.terminateRaftNode(groupId, true);
        }
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.TERMINATE_RAFT_NODES_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(groupIds.size());
        for (CPGroupId groupId : groupIds) {
            out.writeObject(groupId);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int count = in.readInt();
        groupIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            CPGroupId groupId = in.readObject();
            groupIds.add(groupId);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupIds=").append(groupIds);
    }
}
