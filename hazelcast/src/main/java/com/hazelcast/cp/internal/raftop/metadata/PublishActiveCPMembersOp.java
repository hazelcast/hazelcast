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

import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * CP members use this operation to broadcast their current active CP member
 * list to the AP Hazelcast members.
 * <p>
 * Please note that this operation is not a {@link RaftOp},
 * so it is not handled via the Raft layer.
 */
public class PublishActiveCPMembersOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private RaftGroupId metadataGroupId;
    private long membersCommitIndex;
    private Collection<CPMemberInfo> members;

    public PublishActiveCPMembersOp() {
    }

    public PublishActiveCPMembersOp(RaftGroupId metadataGroupId, long membersCommitIndex, Collection<CPMemberInfo> members) {
        this.metadataGroupId = metadataGroupId;
        this.membersCommitIndex = membersCommitIndex;
        this.members = members;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.handleActiveCPMembers(metadataGroupId, membersCommitIndex, members);
    }

    @Override
    public boolean returnsResponse() {
        return false;
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
    public int getClassId() {
        return RaftServiceDataSerializerHook.PUBLISH_ACTIVE_CP_MEMBERS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(metadataGroupId);
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (CPMemberInfo member : members) {
            out.writeObject(member);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        metadataGroupId = in.readObject();
        membersCommitIndex = in.readLong();
        int len = in.readInt();
        members = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            CPMemberInfo member = in.readObject();
            members.add(member);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", metadataGroupId=").append(metadataGroupId)
          .append(", membersCommitIndex").append(membersCommitIndex)
          .append(", members=").append(members);
    }
}
