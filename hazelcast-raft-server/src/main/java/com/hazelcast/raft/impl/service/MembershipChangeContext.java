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

package com.hazelcast.raft.impl.service;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

/**
 * When there is a membership change in the CP sub-system, all decided membership changes of Raft groups are maintained here.
 * {@link RaftGroupMembershipManager} realizes these membership changes.
 *
 * This class is IMMUTABLE because it can be returned as a response to local queries of {@link RaftGroupMembershipManager}
 */
public class MembershipChangeContext implements IdentifiedDataSerializable {

    private RaftMemberImpl leavingMember;
    private final List<RaftGroupMembershipChangeContext> changes = new ArrayList<RaftGroupMembershipChangeContext>();

    MembershipChangeContext() {
    }

    MembershipChangeContext(RaftMemberImpl leavingMember, List<RaftGroupMembershipChangeContext> changes) {
        this.leavingMember = leavingMember;
        this.changes.addAll(changes);
    }

    RaftMemberImpl getLeavingMember() {
        return leavingMember;
    }

    List<RaftGroupMembershipChangeContext> getChanges() {
        return unmodifiableList(changes);
    }

    MembershipChangeContext excludeCompletedChanges(Collection<RaftGroupId> completedGroupIds) {
        checkNotNull(completedGroupIds);

        List<RaftGroupMembershipChangeContext> remainingChanges = new ArrayList<RaftGroupMembershipChangeContext>(changes);
        Iterator<RaftGroupMembershipChangeContext> it = remainingChanges.iterator();
        while (it.hasNext()) {
            RaftGroupMembershipChangeContext ctx = it.next();
            if (completedGroupIds.contains(ctx.groupId)) {
                it.remove();
            }
        }

        return new MembershipChangeContext(leavingMember, remainingChanges);
    }


    public static class RaftGroupMembershipChangeContext implements DataSerializable {

        private RaftGroupId groupId;

        private long membersCommitIndex;

        private Collection<RaftMemberImpl> members;

        private RaftMemberImpl memberToAdd;

        private RaftMemberImpl memberToRemove;

        RaftGroupMembershipChangeContext() {
        }

        RaftGroupMembershipChangeContext(RaftGroupId groupId, long membersCommitIndex, Collection<RaftMemberImpl> members,
                                         RaftMemberImpl memberToAdd, RaftMemberImpl memberToRemove) {
            this.groupId = groupId;
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.memberToAdd = memberToAdd;
            this.memberToRemove = memberToRemove;
        }

        RaftGroupId getGroupId() {
            return groupId;
        }

        long getMembersCommitIndex() {
            return membersCommitIndex;
        }

        Collection<RaftMemberImpl> getMembers() {
            return members;
        }

        RaftMemberImpl getMemberToAdd() {
            return memberToAdd;
        }

        RaftMemberImpl getMemberToRemove() {
            return memberToRemove;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(membersCommitIndex);
            out.writeInt(members.size());
            for (RaftMemberImpl member : members) {
                out.writeObject(member);
            }
            out.writeObject(memberToAdd);
            out.writeObject(memberToRemove);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            membersCommitIndex = in.readLong();
            int len = in.readInt();
            members = new HashSet<RaftMemberImpl>(len);
            for (int i = 0; i < len; i++) {
                RaftMemberImpl member = in.readObject();
                members.add(member);
            }
            memberToAdd = in.readObject();
            memberToRemove = in.readObject();
        }

        @Override
        public String toString() {
            return "RaftGroupMembershipChangeContext{" + "groupId=" + groupId + ", membersCommitIndex=" + membersCommitIndex
                    + ", members=" + members + ", memberToAdd=" + memberToAdd + ", memberToRemove=" + memberToRemove + '}';
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBERSHIP_CHANGE_CTX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(leavingMember);
        out.writeInt(changes.size());
        for (RaftGroupMembershipChangeContext ctx : changes) {
            ctx.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leavingMember = in.readObject();
        int groupCount = in.readInt();
        for (int i = 0; i < groupCount; i++) {
            RaftGroupMembershipChangeContext context = new RaftGroupMembershipChangeContext();
            context.readData(in);
            changes.add(context);
        }
    }

    @Override
    public String toString() {
        return "MembershipChangeContext{" + ", leavingMember=" + leavingMember + ", changes=" + changes + '}';
    }
}
