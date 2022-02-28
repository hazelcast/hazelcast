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
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.MetadataRaftGroupManager;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Commits the given successfully-applied membership changes to the Metadata
 * Raft group. The given changes do not have to contain all pending Raft group
 * membership changes.
 */
public class CompleteRaftGroupMembershipChangesOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware,
                                                                                         IdentifiedDataSerializable {

    private Map<CPGroupId, BiTuple<Long, Long>> changedGroups;

    public CompleteRaftGroupMembershipChangesOp() {
    }

    public CompleteRaftGroupMembershipChangesOp(Map<CPGroupId, BiTuple<Long, Long>> changedGroups) {
        this.changedGroups = changedGroups;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        return metadataGroupManager.completeRaftGroupMembershipChanges(commitIndex, changedGroups);
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
        return RaftServiceDataSerializerHook.COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(changedGroups.size());
        for (Entry<CPGroupId, BiTuple<Long, Long>> e : changedGroups.entrySet()) {
            out.writeObject(e.getKey());
            BiTuple<Long, Long> value = e.getValue();
            out.writeLong(value.element1);
            out.writeLong(value.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        changedGroups = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            CPGroupId groupId = in.readObject();
            long currMembersCommitIndex = in.readLong();
            long newMembersCommitIndex = in.readLong();
            changedGroups.put(groupId, BiTuple.of(currMembersCommitIndex, newMembersCommitIndex));
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", changedGroups=").append(changedGroups);
    }
}
