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

import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Marks the given Raft groups as {@link CPGroupStatus#DESTROYED}
 * and notifies its CP members
 */
public class CompleteDestroyRaftGroupsOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware,
                                                                                IdentifiedDataSerializable {

    private Set<CPGroupId> groupIds;

    public CompleteDestroyRaftGroupsOp() {
    }

    public CompleteDestroyRaftGroupsOp(Set<CPGroupId> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        metadataGroupManager.completeDestroyRaftGroups(groupIds);
        return null;
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
        return RaftServiceDataSerializerHook.COMPLETE_DESTROY_RAFT_GROUPS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(groupIds.size());
        for (CPGroupId groupId : groupIds) {
            out.writeObject(groupId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        groupIds = new HashSet<>();
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
