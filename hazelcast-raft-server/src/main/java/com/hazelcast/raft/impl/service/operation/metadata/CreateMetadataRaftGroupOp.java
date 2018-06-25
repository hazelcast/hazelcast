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
import com.hazelcast.raft.impl.service.MetadataRaftGroupManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used during cluster startup by CP nodes to commit their CP node list to the Metadata group.
 * in order to guarantee that each CP node discovers the same list.
 * Fails with {@link IllegalArgumentException} if a CP node commits a different list.
 */
public class CreateMetadataRaftGroupOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private List<RaftMemberImpl> initialMembers;
    private int metadataMembersCount;

    public CreateMetadataRaftGroupOp() {
    }

    public CreateMetadataRaftGroupOp(List<RaftMemberImpl> initialMembers, int metadataMembersCount) {
        this.initialMembers = initialMembers;
        this.metadataMembersCount = metadataMembersCount;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        metadataManager.createInitialMetadataRaftGroup(initialMembers, metadataMembersCount);
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
        return RaftServiceDataSerializerHook.CREATE_METADATA_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(initialMembers.size());
        for (RaftMemberImpl member : initialMembers) {
            out.writeObject(member);
        }
        out.writeInt(metadataMembersCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        initialMembers = new ArrayList<RaftMemberImpl>(len);
        for (int i = 0; i < len; i++) {
            RaftMemberImpl member = in.readObject();
            initialMembers.add(member);
        }
        metadataMembersCount = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append("members=").append(metadataMembersCount);
    }
}
