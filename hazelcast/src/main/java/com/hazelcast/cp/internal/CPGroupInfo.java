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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPMember;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.hazelcast.cp.CPGroup.CPGroupStatus.ACTIVE;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYED;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYING;
import static com.hazelcast.util.Preconditions.checkState;

/**
 * Contains metadata information for Raft groups, such as group id,
 * group members, etc. Maintained within the Metadata Raft group.
 */
public final class CPGroupInfo implements CPGroup, IdentifiedDataSerializable {

    private RaftGroupId id;
    private Set<CPMemberInfo> initialMembers;
    private Set<CPMemberInfo> members;
    private long membersCommitIndex;

    // read outside of Raft
    private volatile CPGroupStatus status;

    private transient CPMemberInfo[] membersArray;

    public CPGroupInfo() {
    }

    public CPGroupInfo(RaftGroupId id, Collection<CPMemberInfo> members) {
        this.id = id;
        this.status = ACTIVE;
        this.initialMembers = Collections.unmodifiableSet(new LinkedHashSet<CPMemberInfo>(members));
        this.members = Collections.unmodifiableSet(new LinkedHashSet<CPMemberInfo>(members));
        this.membersArray = members.toArray(new CPMemberInfo[0]);
    }

    @Override
    public RaftGroupId id() {
        return id;
    }

    public String name() {
        return id.name();
    }

    public int initialMemberCount() {
        return initialMembers.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<CPMember> members() {
        return (Collection) members;
    }

    public Collection<CPMemberInfo> memberImpls() {
        return members;
    }

    @SuppressWarnings("unchecked")
    public Collection<CPMember> initialMembers() {
        return (Collection) initialMembers;
    }

    public boolean containsMember(CPMemberInfo member) {
        return members.contains(member);
    }

    public int memberCount() {
        return members.size();
    }

    @Override
    public CPGroupStatus status() {
        return status;
    }

    public boolean setDestroying() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYING;
        return true;
    }

    public boolean setDestroyed() {
        checkState(status != ACTIVE, "Cannot destroy " + id + " because status is: " + status);
        return forceSetDestroyed();
    }

    public boolean forceSetDestroyed() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYED;
        return true;
    }

    public long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public boolean applyMembershipChange(CPMemberInfo leaving, CPMemberInfo joining,
                                         long expectedMembersCommitIndex, long newMembersCommitIndex) {
        checkState(status == ACTIVE, "Cannot apply membership change of Leave: " + leaving
                + " and Join: " + joining + " since status is: " + status);
        if (membersCommitIndex != expectedMembersCommitIndex) {
            return false;
        }

        Set<CPMemberInfo> m = new LinkedHashSet<CPMemberInfo>(members);
        if (leaving != null) {
            boolean removed = m.remove(leaving);
            assert removed : leaving + " is not member of " + toString();
        }

        if (joining != null) {
            boolean added = m.add(joining);
            assert added : joining + " is already member of " + toString();
        }

        members = Collections.unmodifiableSet(m);
        membersCommitIndex = newMembersCommitIndex;
        membersArray = members.toArray(new CPMemberInfo[0]);
        return true;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
            justification = "Returning internal array intentionally to avoid performance penalty.")
    public CPMemberInfo[] membersArray() {
        return membersArray;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeInt(initialMembers.size());
        for (CPMemberInfo member : initialMembers) {
            out.writeObject(member);
        }
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (CPMemberInfo member : members) {
            out.writeObject(member);
        }
        out.writeUTF(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        int initialMemberCount = in.readInt();
        Set<CPMemberInfo> initialMembers = new LinkedHashSet<CPMemberInfo>();
        for (int i = 0; i < initialMemberCount; i++) {
            CPMemberInfo member = in.readObject();
            initialMembers.add(member);
        }
        this.initialMembers = Collections.unmodifiableSet(initialMembers);
        membersCommitIndex = in.readLong();
        int memberCount = in.readInt();
        members = new LinkedHashSet<CPMemberInfo>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            CPMemberInfo member = in.readObject();
            members.add(member);
        }
        membersArray = members.toArray(new CPMemberInfo[0]);
        members = Collections.unmodifiableSet(members);
        status = CPGroupStatus.valueOf(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.RAFT_GROUP_INFO;
    }

    @Override
    public String toString() {
        return "CPGroupInfo{" + "id=" + id + ", initialMembers=" + initialMembers + ", membersCommitIndex=" + membersCommitIndex
                + ", members=" + members() + ", status=" + status + '}';
    }
}
