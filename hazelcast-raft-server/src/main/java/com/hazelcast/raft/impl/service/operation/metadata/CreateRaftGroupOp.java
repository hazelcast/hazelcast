/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.MetadataRaftGroupManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.exception.CannotCreateRaftGroupException;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Creates a new Raft group with the given name and the members and returns its ID.
 * Expected number of members for the Raft group is validated via the Hazelcast configuration. Number of members in the operation
 * are expected to be same with the Raft group member count in the configuration.
 * If an active Raft group exists for the same name, we check if the Raft group contains the same number of members.
 * If group size is same, we return ID of the existing Raft group.
 * Otherwise, we fail with {@link IllegalStateException} and this exception will be exposed to the user.
 * If a member in the given member list is not an active CP node, the operation fails with {@link CannotCreateRaftGroupException}.
 * This exception will be handled by {@link RaftInvocationManager} and another attempt will be made with a new member list.
 * <p/>
 * This operation is committed to the Metadata group.
 */
public class CreateRaftGroupOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String groupName;
    private Collection<RaftMemberImpl> members;

    public CreateRaftGroupOp() {
    }

    public CreateRaftGroupOp(String groupName, Collection<RaftMemberImpl> members) {
        this.groupName = groupName;
        this.members = members;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        return metadataManager.createRaftGroup(groupName, members, commitIndex);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeInt(members.size());
        for (RaftMemberImpl member : members) {
            out.writeObject(member);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        int len = in.readInt();
        members = new ArrayList<RaftMemberImpl>(len);
        for (int i = 0; i < len; i++) {
            RaftMemberImpl member = in.readObject();
            members.add(member);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupName=").append(groupName)
          .append(", members=").append(members);
    }
}
