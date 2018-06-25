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
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;

import java.io.IOException;

import static com.hazelcast.raft.impl.service.MetadataRaftGroupManager.METADATA_GROUP_ID;

/**
 * A {@link RaftOp} that adds a new CP member to the CP sub-system.
 * Committed to the Metadata Raft group.
 * Fails with {@link IllegalArgumentException} if the member to be added is already a CP member that is currently being removed.
 */
public class AddRaftMemberOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private RaftMemberImpl member;

    public AddRaftMemberOp() {
    }

    public AddRaftMemberOp(RaftMemberImpl member) {
        this.member = member;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        assert METADATA_GROUP_ID.equals(groupId);
        RaftService service = getService();
        service.getMetadataGroupManager().addActiveMember(member);
        return null;
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
        return RaftServiceDataSerializerHook.ADD_RAFT_MEMBER_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", member=").append(member);
    }
}
