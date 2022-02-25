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

import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.util.PostponedResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used during cluster startup by CP members to commit their CP member list
 * to the Metadata group. in order to guarantee that each CP member discovers
 * the same list. Fails with {@link IllegalArgumentException} if a CP member
 * commits a different list.
 */
public class InitMetadataRaftGroupOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware,
                                                                            IdentifiedDataSerializable {

    private CPMemberInfo callerCPMember;
    private List<CPMemberInfo> discoveredCPMembers;
    private long groupIdSeed;

    public InitMetadataRaftGroupOp() {
    }

    public InitMetadataRaftGroupOp(CPMemberInfo callerCPMember, List<CPMemberInfo> discoveredCPMembers, long groupIdSeed) {
        this.callerCPMember = callerCPMember;
        this.discoveredCPMembers = discoveredCPMembers;
        this.groupIdSeed = groupIdSeed;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        if (metadataGroupManager.initMetadataGroup(commitIndex, callerCPMember, discoveredCPMembers, groupIdSeed)) {
            return null;
        }

        return PostponedResponse.INSTANCE;
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
        return RaftServiceDataSerializerHook.INIT_METADATA_RAFT_GROUP_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(callerCPMember);
        out.writeInt(discoveredCPMembers.size());
        for (CPMemberInfo member : discoveredCPMembers) {
            out.writeObject(member);
        }
        out.writeLong(groupIdSeed);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        callerCPMember = in.readObject();
        int len = in.readInt();
        discoveredCPMembers = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            CPMemberInfo member = in.readObject();
            discoveredCPMembers.add(member);
        }
        groupIdSeed = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", callerCPMember=").append(callerCPMember)
          .append(", discoveredCPMembers=").append(discoveredCPMembers)
          .append(", groupIdSeed=").append(groupIdSeed);
    }
}
