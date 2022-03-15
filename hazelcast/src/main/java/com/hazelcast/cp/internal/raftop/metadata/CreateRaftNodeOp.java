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
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * On creation of a new Raft group or a membership change in an existing Raft
 * group, this operation is sent to the new members of the Raft group to
 * initiate the {@link RaftNode} on the new member. Members present in this
 * operation are initial members of the Raft group, not the current members.
 * <p>
 * Please note that this operation is not a {@link RaftOp},
 * so it is not handled via the Raft layer.
 */
public class CreateRaftNodeOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private CPGroupId groupId;
    private Collection<RaftEndpoint> initialMembers;

    public CreateRaftNodeOp() {
    }

    public CreateRaftNodeOp(CPGroupId groupId, Collection<RaftEndpoint> initialMembers) {
        this.groupId = groupId;
        this.initialMembers = initialMembers;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.createRaftNode(groupId, initialMembers);
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
        return RaftServiceDataSerializerHook.CREATE_RAFT_NODE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeInt(initialMembers.size());
        for (RaftEndpoint member : initialMembers) {
            out.writeObject(member);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        int count = in.readInt();
        initialMembers = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            RaftEndpoint member = in.readObject();
            initialMembers.add(member);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupId=").append(groupId)
          .append(", initialMembers=").append(initialMembers);
    }
}
