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

import com.hazelcast.cp.CPGroup.CPGroupStatus;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cp.CPGroup.CPGroupStatus.ACTIVE;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYED;
import static com.hazelcast.cp.CPGroup.CPGroupStatus.DESTROYING;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static java.util.Collections.unmodifiableSet;

/**
 * Contains metadata information for Raft groups, such as group id,
 * group members, etc. Maintained within the Metadata Raft group.
 */
public final class CPGroupInfo implements IdentifiedDataSerializable {

    private RaftGroupId id;
    private Set<RaftEndpoint> initialMembers;
    private Set<RaftEndpoint> members;
    private long membersCommitIndex;

    // read outside of Raft
    private volatile CPGroupStatus status;

    public CPGroupInfo() {
    }

    public CPGroupInfo(RaftGroupId id, Collection<RaftEndpoint> members) {
        this.id = id;
        this.status = ACTIVE;
        this.initialMembers = unmodifiableSet(new LinkedHashSet<>(members));
        this.members = unmodifiableSet(new LinkedHashSet<>(members));
    }

    // Copy constructor
    CPGroupInfo(CPGroupInfo other) {
        this.id = other.id;
        this.status = other.status;
        this.membersCommitIndex = other.membersCommitIndex;
        this.initialMembers = Collections.unmodifiableSet(new LinkedHashSet<>(other.initialMembers));
        this.members = Collections.unmodifiableSet(new LinkedHashSet<>(other.members));
    }

    public RaftGroupId id() {
        return id;
    }

    public String name() {
        return id.getName();
    }

    public int initialMemberCount() {
        return initialMembers.size();
    }

    public Collection<RaftEndpoint> members() {
        return members;
    }

    public int memberCount() {
        return members.size();
    }

    public boolean containsMember(RaftEndpoint member) {
        return members.contains(member);
    }

    public Collection<RaftEndpoint> memberImpls() {
        return members;
    }

    public Collection<RaftEndpoint> initialMembers() {
        return initialMembers;
    }

    public CPGroupStatus status() {
        return status;
    }

    boolean setDestroying() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYING;
        return true;
    }

    boolean setDestroyed() {
        checkState(status != ACTIVE, "Cannot destroy " + id + " because status is: " + status);
        return forceSetDestroyed();
    }

    boolean forceSetDestroyed() {
        if (status == DESTROYED) {
            return false;
        }

        status = DESTROYED;
        return true;
    }

    long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    boolean applyMembershipChange(RaftEndpoint leaving, RaftEndpoint joining, long expectedMembersCommitIndex,
                                  long newMembersCommitIndex) {
        checkState(status == ACTIVE, "Cannot apply membership change of Leave: " + leaving
                + " and Join: " + joining + " since status is: " + status);
        if (membersCommitIndex != expectedMembersCommitIndex) {
            return false;
        }

        Set<RaftEndpoint> m = new LinkedHashSet<>(members);
        if (leaving != null) {
            boolean removed = m.remove(leaving);
            assert removed : leaving + " is not member of " + toString();
        }

        if (joining != null) {
            boolean added = m.add(joining);
            assert added : joining + " is already member of " + toString();
        }

        members = unmodifiableSet(m);
        membersCommitIndex = newMembersCommitIndex;
        return true;
    }

    CPGroupSummary toSummary(Collection<CPMemberInfo> cpMembers) {
        Map<UUID, CPMemberInfo> cpMembersMap = new HashMap<>();
        for (CPMemberInfo cpMember : cpMembers) {
            cpMembersMap.put(cpMember.getUuid(), cpMember);
        }
        // we should preserve the member ordering so we iterate over group members instead of all cp members
        List<CPMember> groupEndpoints = new ArrayList<>();
        for (RaftEndpoint endpoint : members) {
            CPMemberInfo memberInfo = cpMembersMap.get(endpoint.getUuid());
            if (memberInfo == null) {
                continue;
            }
            groupEndpoints.add(memberInfo);
        }

        if (groupEndpoints.size() != members.size()) {
            throw new IllegalStateException("Missing CP member in active CP members: " + cpMembers + " for " + this);
        }

        return new CPGroupSummary(id,  status, initialMembers, groupEndpoints);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeInt(initialMembers.size());
        for (RaftEndpoint member : initialMembers) {
            out.writeObject(member);
        }
        out.writeLong(membersCommitIndex);
        out.writeInt(members.size());
        for (RaftEndpoint member : members) {
            out.writeObject(member);
        }
        out.writeString(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        int initialMemberCount = in.readInt();
        Set<RaftEndpoint> initialMembers = new LinkedHashSet<>();
        for (int i = 0; i < initialMemberCount; i++) {
            RaftEndpoint member = in.readObject();
            initialMembers.add(member);
        }
        this.initialMembers = unmodifiableSet(initialMembers);
        membersCommitIndex = in.readLong();
        int memberCount = in.readInt();
        members = new LinkedHashSet<>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            RaftEndpoint member = in.readObject();
            members.add(member);
        }
        members = unmodifiableSet(members);
        status = CPGroupStatus.valueOf(in.readString());
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.RAFT_GROUP_INFO;
    }

    @Override
    public String toString() {
        return "CPGroupInfo{" + "id=" + id + ", initialMembers=" + initialMembers + ", membersCommitIndex=" + membersCommitIndex
                + ", members=" + members() + ", status=" + status + '}';
    }
}
