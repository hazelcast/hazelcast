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

import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Creates a new Raft group with the given name and the members and returns
 * its ID. Expected number of members for the Raft group is validated via
 * the Hazelcast configuration. Number of members in the operation are expected
 * to be same with the Raft group member count in the configuration.
 * If an active Raft group exists for the same name, we check if the Raft group
 * contains the same number of members. If group size is same, we return
 * ID of the existing Raft group. Otherwise, we fail
 * with {@link IllegalStateException} and this exception will be exposed to
 * the user. If a member in the given member list is not an active CP member,
 * the operation fails with {@link CannotCreateRaftGroupException}.
 * This exception will be handled by {@link RaftInvocationManager} and another
 * attempt will be made with a new member list.
 * <p>
 * This operation is committed to the Metadata group.
 */
public class CreateRaftGroupOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware,
                                                                      IdentifiedDataSerializable {

    private String groupName;
    private Collection<RaftEndpoint> members;
    private long groupId;

    public CreateRaftGroupOp() {
    }

    public CreateRaftGroupOp(String groupName, Collection<RaftEndpoint> members, long groupId) {
        this.groupName = groupName;
        this.members = members;
        this.groupId = groupId;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        return metadataGroupManager.createRaftGroup(groupName, members, groupId);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(groupName);
        out.writeInt(members.size());
        for (RaftEndpoint member : members) {
            out.writeObject(member);
        }
        out.writeLong(groupId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readString();
        int len = in.readInt();
        members = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint member = in.readObject();
            members.add(member);
        }
        groupId = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupName=").append(groupName)
          .append(", members=").append(members)
          .append(", groupIndex=").append(groupId);
    }
}
