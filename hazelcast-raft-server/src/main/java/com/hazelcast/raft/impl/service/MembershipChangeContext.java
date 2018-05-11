/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkFalse;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkState;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * TODO: Javadoc Pending...
 *
 * This class is IMMUTABLE because it can be returned as a response to local queries of {@link RaftCleanupHandler}
 */
public class MembershipChangeContext implements IdentifiedDataSerializable {

    private final Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups = new HashMap<RaftGroupId, List<RaftMemberImpl>>();
    private RaftMemberImpl leavingMember;
    private final List<RaftGroupMembershipChangeContext> changes = new ArrayList<RaftGroupMembershipChangeContext>();

    public MembershipChangeContext() {
    }

    public MembershipChangeContext(Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups) {
        this(memberMissingGroups, null, Collections.<RaftGroupMembershipChangeContext>emptyList());
    }

    public MembershipChangeContext(RaftMemberImpl leavingMember, List<RaftGroupMembershipChangeContext> changes) {
        this(Collections.<RaftGroupId, List<RaftMemberImpl>>emptyMap(), leavingMember, changes);
    }

    private MembershipChangeContext(Map<RaftGroupId, List<RaftMemberImpl>> memberMissingGroups, RaftMemberImpl leavingMember,
                                    List<RaftGroupMembershipChangeContext> changes) {
        this.memberMissingGroups.putAll(memberMissingGroups);
        this.leavingMember = leavingMember;
        this.changes.addAll(changes);
    }

    public Map<RaftGroupId, List<RaftMemberImpl>> getMemberMissingGroups() {
        return unmodifiableMap(memberMissingGroups);
    }

    public boolean hasNoPendingMembersToAdd() {
        return memberMissingGroups.isEmpty();
    }

    public RaftMemberImpl getLeavingMember() {
        return leavingMember;
    }

    public List<RaftGroupMembershipChangeContext> getChanges() {
        return unmodifiableList(changes);
    }

    public boolean hasNoPendingChanges() {
        return changes.isEmpty();
    }

    public MembershipChangeContext setChanges(List<RaftGroupMembershipChangeContext> newChanges) {
        checkNotNull(newChanges);
        checkFalse(newChanges.isEmpty(), "Cannot set empty changes for " + this);
        checkState(leavingMember == null,
                "Cannot set changes: " + newChanges + " because there is a leaving member for " + this);
        checkState(changes.isEmpty(),
                "Cannot set changes: " + newChanges + " because there are pending membership for: " + this);
        Set<RaftGroupId> groupIds = new HashSet<RaftGroupId>(memberMissingGroups.keySet());
        for (RaftGroupMembershipChangeContext ctx : newChanges) {
            groupIds.remove(ctx.groupId);
        }
        checkState(groupIds.isEmpty(), "Changes: " + newChanges + " do not cover all missing member groups for " + this);

        return new MembershipChangeContext(leavingMember, newChanges);
    }

    public MembershipChangeContext excludeCompletedChanges(Collection<RaftGroupId> completedGroupIds) {
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

        public RaftGroupMembershipChangeContext() {
        }

        public RaftGroupMembershipChangeContext(RaftGroupId groupId, long membersCommitIndex, Collection<RaftMemberImpl> members,
                                                RaftMemberImpl memberToAdd, RaftMemberImpl memberToRemove) {
            this.groupId = groupId;
            this.membersCommitIndex = membersCommitIndex;
            this.members = members;
            this.memberToAdd = memberToAdd;
            this.memberToRemove = memberToRemove;
        }

        public RaftGroupId getGroupId() {
            return groupId;
        }

        public long getMembersCommitIndex() {
            return membersCommitIndex;
        }

        public Collection<RaftMemberImpl> getMembers() {
            return members;
        }

        public RaftMemberImpl getMemberToAdd() {
            return memberToAdd;
        }

        public RaftMemberImpl getMemberToRemove() {
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
        out.writeInt(memberMissingGroups.size());
        for (Entry<RaftGroupId, List<RaftMemberImpl>> e : memberMissingGroups.entrySet()) {
            out.writeObject(e.getKey());
            List<RaftMemberImpl> members = e.getValue();
            out.writeInt(members.size());
            for (RaftMemberImpl member : members) {
                out.writeObject(member);
            }
        }
        out.writeObject(leavingMember);
        out.writeInt(changes.size());
        for (RaftGroupMembershipChangeContext ctx : changes) {
            ctx.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int missingMemberGroupCount = in.readInt();
        for (int i = 0; i < missingMemberGroupCount; i++) {
            RaftGroupId id = in.readObject();
            int memberCount = in.readInt();
            List<RaftMemberImpl> members = new ArrayList<RaftMemberImpl>(memberCount);
            for (int j = 0; j < memberCount; j++) {
                RaftMemberImpl member = in.readObject();
                members.add(member);
            }
            memberMissingGroups.put(id, members);
        }
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
        return "MembershipChangeContext{" + "memberMissingGroups=" + memberMissingGroups + ", leavingMember="
                + leavingMember + ", changes=" + changes + '}';
    }
}
