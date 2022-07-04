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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

/**
 * When there is a membership change in CP Subsystem,
 * all decided membership changes of Raft groups are maintained here.
 * {@link RaftGroupMembershipManager} realizes these membership changes.
 *
 * This class is IMMUTABLE because it can be returned as a response to
 * local queries of {@link RaftGroupMembershipManager}
 */
public class MembershipChangeSchedule implements IdentifiedDataSerializable {

    private List<Long> membershipChangeCommitIndices;
    private CPMemberInfo member;
    private MembershipChangeMode membershipChangeMode;
    private final List<CPGroupMembershipChange> changes = new ArrayList<>();

    MembershipChangeSchedule() {
    }

    private MembershipChangeSchedule(List<Long> membershipChangeCommitIndices, CPMemberInfo member,
                                     MembershipChangeMode membershipChangeMode, List<CPGroupMembershipChange> changes) {
        this.membershipChangeCommitIndices = membershipChangeCommitIndices;
        this.member = member;
        this.membershipChangeMode = membershipChangeMode;
        this.changes.addAll(changes);
    }

    CPMemberInfo getAddedMember() {
        return membershipChangeMode == MembershipChangeMode.ADD ? member : null;
    }

    CPMemberInfo getLeavingMember() {
        return membershipChangeMode == MembershipChangeMode.REMOVE ? member : null;
    }

    List<CPGroupMembershipChange> getChanges() {
        return unmodifiableList(changes);
    }

    MembershipChangeSchedule excludeCompletedChanges(Collection<CPGroupId> completedGroupIds) {
        checkNotNull(completedGroupIds);

        List<CPGroupMembershipChange> remainingChanges = new ArrayList<>(changes);
        remainingChanges.removeIf(change -> completedGroupIds.contains(change.groupId));

        return new MembershipChangeSchedule(membershipChangeCommitIndices, member, membershipChangeMode, remainingChanges);
    }

    List<Long> getMembershipChangeCommitIndices() {
        return membershipChangeCommitIndices;
    }

    MembershipChangeSchedule addRetriedCommitIndex(long commitIndex) {
        List<Long> membershipChangeCommitIndices = new ArrayList<>(this.membershipChangeCommitIndices);
        membershipChangeCommitIndices.add(commitIndex);
        return new MembershipChangeSchedule(membershipChangeCommitIndices, member, membershipChangeMode, changes);
    }

    static MembershipChangeSchedule forJoiningMember(List<Long> membershipChangeCommitIndices, CPMemberInfo member,
                                                     List<CPGroupMembershipChange> changes) {
        return new MembershipChangeSchedule(membershipChangeCommitIndices, member, MembershipChangeMode.ADD, changes);
    }

    static MembershipChangeSchedule forLeavingMember(List<Long> membershipChangeCommitIndices, CPMemberInfo member,
                                                     List<CPGroupMembershipChange> changes) {
        return new MembershipChangeSchedule(membershipChangeCommitIndices, member, MembershipChangeMode.REMOVE, changes);
    }

    /**
     * Contains a membership change that will be performed on a CP group
     */
    public static class CPGroupMembershipChange implements IdentifiedDataSerializable {

        private CPGroupId groupId;

        private long membersCommitIndex;

        private Collection<RaftEndpoint> members;

        private RaftEndpoint memberToAdd;

        private RaftEndpoint memberToRemove;

        CPGroupMembershipChange() {
        }

        CPGroupMembershipChange(CPGroupId groupId, long membersCommitIndex, Collection<RaftEndpoint> members,
                                RaftEndpoint memberToAdd, RaftEndpoint memberToRemove) {
            this.groupId = groupId;
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.memberToAdd = memberToAdd;
            this.memberToRemove = memberToRemove;
        }

        CPGroupId getGroupId() {
            return groupId;
        }

        long getMembersCommitIndex() {
            return membersCommitIndex;
        }

        Collection<RaftEndpoint> getMembers() {
            return members;
        }

        RaftEndpoint getMemberToAdd() {
            return memberToAdd;
        }

        RaftEndpoint getMemberToRemove() {
            return memberToRemove;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(groupId);
            out.writeLong(membersCommitIndex);
            out.writeInt(members.size());
            for (RaftEndpoint member : members) {
                out.writeObject(member);
            }
            out.writeObject(memberToAdd);
            out.writeObject(memberToRemove);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            groupId = in.readObject();
            membersCommitIndex = in.readLong();
            int len = in.readInt();
            members = new HashSet<>(len);
            for (int i = 0; i < len; i++) {
                RaftEndpoint member = in.readObject();
                members.add(member);
            }
            memberToAdd = in.readObject();
            memberToRemove = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return RaftServiceDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return RaftServiceDataSerializerHook.GROUP_MEMBERSHIP_CHANGE;
        }

        @Override
        public String toString() {
            return "CPGroupMembershipChange{" + "groupId=" + groupId + ", membersCommitIndex=" + membersCommitIndex
                    + ", members=" + members + ", memberToAdd=" + memberToAdd + ", memberToRemove=" + memberToRemove + '}';
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_SCHEDULE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(membershipChangeCommitIndices.size());
        for (long commitIndex : membershipChangeCommitIndices) {
            out.writeLong(commitIndex);
        }
        out.writeObject(member);
        out.writeString(membershipChangeMode.name());
        out.writeInt(changes.size());
        for (CPGroupMembershipChange change : changes) {
            out.writeObject(change);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int membershipChangeCommitIndexCount = in.readInt();
        membershipChangeCommitIndices = new ArrayList<>(membershipChangeCommitIndexCount);
        for (int i = 0; i < membershipChangeCommitIndexCount; i++) {
            long commitIndex = in.readLong();
            membershipChangeCommitIndices.add(commitIndex);
        }
        member = in.readObject();
        membershipChangeMode = MembershipChangeMode.valueOf(in.readString());
        int groupCount = in.readInt();
        for (int i = 0; i < groupCount; i++) {
            CPGroupMembershipChange change = in.readObject();
            changes.add(change);
        }
    }

    @Override
    public String toString() {
        return "MembershipChangeSchedule{" + "membershipChangeCommitIndices=" + membershipChangeCommitIndices + ", member="
                + member + ", membershipChangeMode=" + membershipChangeMode + ", changes=" + changes + '}';
    }
}
