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

package com.hazelcast.cp.internal.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.io.IOException;

/**
 * Replicates a membership change operation to a Raft group.
 */
public class ChangeRaftGroupMembershipOp extends RaftReplicateOp implements IndeterminateOperationStateAware,
                                                                            IdentifiedDataSerializable {

    private static final int NAN_MEMBERS_COMMIT_INDEX = -1;

    private long membersCommitIndex;
    private RaftEndpoint member;
    private MembershipChangeMode membershipChangeMode;

    public ChangeRaftGroupMembershipOp() {
    }

    public ChangeRaftGroupMembershipOp(CPGroupId groupId, long membersCommitIndex, RaftEndpoint member,
                                       MembershipChangeMode membershipChangeMode) {
        super(groupId);
        this.membersCommitIndex = membersCommitIndex;
        this.member = member;
        this.membershipChangeMode = membershipChangeMode;
    }

    @Override
    protected InternalCompletableFuture replicate(RaftNode raftNode) {
        if (membersCommitIndex == NAN_MEMBERS_COMMIT_INDEX) {
            return raftNode.replicateMembershipChange(member, membershipChangeMode);
        } else {
            return raftNode.replicateMembershipChange(member, membershipChangeMode, membersCommitIndex);
        }
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
    public int getClassId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(membersCommitIndex);
        out.writeObject(member);
        out.writeString(membershipChangeMode.toString());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        membersCommitIndex = in.readLong();
        member = in.readObject();
        membershipChangeMode = MembershipChangeMode.valueOf(in.readString());
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", membersCommitIndex=").append(membersCommitIndex)
          .append(", member=").append(member)
          .append(", membershipChangeMode=").append(membershipChangeMode);
    }
}
