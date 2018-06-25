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

package com.hazelcast.raft.impl.service.proxy;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.MembershipChangeType;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;

/**
 * Replicates a membership change operation to a Raft group.
 */
public class ChangeRaftGroupMembershipOp extends RaftReplicateOp implements IndeterminateOperationStateAware,
                                                                            IdentifiedDataSerializable {

    private static final int NAN_MEMBERS_COMMIT_INDEX = -1;

    private long membersCommitIndex;
    private RaftMemberImpl member;
    private MembershipChangeType changeType;

    public ChangeRaftGroupMembershipOp() {
    }

    public ChangeRaftGroupMembershipOp(RaftGroupId groupId, long membersCommitIndex, RaftMemberImpl member,
                                       MembershipChangeType changeType) {
        super(groupId);
        this.membersCommitIndex = membersCommitIndex;
        this.member = member;
        this.changeType = changeType;
    }

    @Override
    ICompletableFuture replicate(RaftNode raftNode) {
        if (membersCommitIndex == NAN_MEMBERS_COMMIT_INDEX) {
            return raftNode.replicateMembershipChange(member, changeType);
        } else {
            return raftNode.replicateMembershipChange(member, changeType, membersCommitIndex);
        }
    }

    @Override
    protected RaftOp getRaftOp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(membersCommitIndex);
        out.writeObject(member);
        out.writeUTF(changeType.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        membersCommitIndex = in.readLong();
        member = in.readObject();
        changeType = MembershipChangeType.valueOf(in.readUTF());
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", membersCommitIndex=").append(membersCommitIndex)
          .append(", member=").append(member)
          .append(", changeType=").append(changeType);
    }
}
