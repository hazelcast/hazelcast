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

package com.hazelcast.cp.internal.raftop.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used during cluster startup by CP members to commit their CP member list
 * to the Metadata group. in order to guarantee that each CP member discovers
 * the same list. Fails with {@link IllegalArgumentException} if a CP member
 * commits a different list.
 */
public class InitializeMetadataRaftGroupOp extends RaftOp implements IndeterminateOperationStateAware,
                                                                     IdentifiedDataSerializable {

    private List<CPMemberInfo> initialMembers;
    private int metadataMembersCount;
    private long groupIdSeed;

    public InitializeMetadataRaftGroupOp() {
    }

    public InitializeMetadataRaftGroupOp(List<CPMemberInfo> initialMembers, int metadataMembersCount, long groupIdSeed) {
        this.initialMembers = initialMembers;
        this.metadataMembersCount = metadataMembersCount;
        this.groupIdSeed = groupIdSeed;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        metadataManager.initializeMetadataRaftGroup(initialMembers, metadataMembersCount, groupIdSeed);
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
        return RaftServiceDataSerializerHook.INITIALIZE_METADATA_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(initialMembers.size());
        for (CPMemberInfo member : initialMembers) {
            out.writeObject(member);
        }
        out.writeInt(metadataMembersCount);
        out.writeLong(groupIdSeed);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        initialMembers = new ArrayList<CPMemberInfo>(len);
        for (int i = 0; i < len; i++) {
            CPMemberInfo member = in.readObject();
            initialMembers.add(member);
        }
        metadataMembersCount = in.readInt();
        groupIdSeed = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append("members=").append(initialMembers)
          .append(", metadataMemberCount=").append(metadataMembersCount)
          .append(", groupIdSeed=").append(groupIdSeed);
    }
}
