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
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.MetadataRaftGroupManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.impl.util.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Commits the given successfully-applied membership changes to the Metadata Raft group.
 * The given changes do not have to contain all pending Raft group membership changes.
 */
public class CompleteRaftGroupMembershipChangesOp extends RaftOp implements IndeterminateOperationStateAware,
                                                                            IdentifiedDataSerializable {

    private Map<RaftGroupId, Tuple2<Long, Long>> changedGroups;

    public CompleteRaftGroupMembershipChangesOp() {
    }

    public CompleteRaftGroupMembershipChangesOp(Map<RaftGroupId, Tuple2<Long, Long>> changedGroups) {
        this.changedGroups = changedGroups;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        MetadataRaftGroupManager metadataManager = service.getMetadataGroupManager();
        return metadataManager.completeRaftGroupMembershipChanges(changedGroups);
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
        return RaftServiceDataSerializerHook.COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(changedGroups.size());
        for (Entry<RaftGroupId, Tuple2<Long, Long>> e : changedGroups.entrySet()) {
            out.writeObject(e.getKey());
            Tuple2<Long, Long> value = e.getValue();
            out.writeLong(value.element1);
            out.writeLong(value.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        changedGroups = new HashMap<RaftGroupId, Tuple2<Long, Long>>(count);
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            long currMembersCommitIndex = in.readLong();
            long newMembersCommitIndex = in.readLong();
            changedGroups.put(groupId, Tuple2.of(currMembersCommitIndex, newMembersCommitIndex));
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", changedGroups=").append(changedGroups);
    }
}
