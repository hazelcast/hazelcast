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

import com.hazelcast.cp.internal.MetadataRaftGroupManager.MetadataRaftGroupInitStatus;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Snapshot of the Metadata Raft group state
 */
public final class MetadataRaftGroupSnapshot implements IdentifiedDataSerializable {

    private final Collection<CPMemberInfo> members = new ArrayList<>();
    private long membersCommitIndex;
    private final Collection<CPGroupInfo> groups = new ArrayList<>();
    private MembershipChangeSchedule membershipChangeSchedule;
    private List<CPMemberInfo> initialCPMembers;
    private Set<CPMemberInfo> initializedCPMembers = new HashSet<>();
    private MetadataRaftGroupInitStatus initializationStatus;
    private Set<Long> initializationCommitIndices = new HashSet<>();

    public void setGroups(Collection<CPGroupInfo> groups) {
        for (CPGroupInfo group : groups) {
            // Deep copy CPGroupInfo, because it's a mutable object.
            this.groups.add(new CPGroupInfo(group));
        }
    }

    public void setMembers(Collection<CPMemberInfo> members) {
        this.members.addAll(members);
    }

    public Collection<CPMemberInfo> getMembers() {
        return members;
    }

    public long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public void setMembersCommitIndex(long membersCommitIndex) {
        this.membersCommitIndex = membersCommitIndex;
    }

    public Collection<CPGroupInfo> getGroups() {
        return groups;
    }

    public MembershipChangeSchedule getMembershipChangeSchedule() {
        return membershipChangeSchedule;
    }

    public void setMembershipChangeSchedule(MembershipChangeSchedule membershipChangeSchedule) {
        this.membershipChangeSchedule = membershipChangeSchedule;
    }

    public Set<CPMemberInfo> getInitializedCPMembers() {
        return initializedCPMembers;
    }

    public void setInitializedCPMembers(Collection<CPMemberInfo> initializedCPMembers) {
        this.initializedCPMembers.addAll(initializedCPMembers);
    }

    public List<CPMemberInfo> getInitialCPMembers() {
        return initialCPMembers;
    }

    public void setInitialCPMembers(List<CPMemberInfo> initialCPMembers) {
        this.initialCPMembers = initialCPMembers;
    }

    public MetadataRaftGroupInitStatus getInitializationStatus() {
        return initializationStatus;
    }

    public void setInitializationStatus(MetadataRaftGroupInitStatus initializationStatus) {
        this.initializationStatus = initializationStatus;
    }

    public Set<Long> getInitializationCommitIndices() {
        return initializationCommitIndices;
    }

    public void setInitializationCommitIndices(Set<Long> initializationCommitIndices) {
        this.initializationCommitIndices.addAll(initializationCommitIndices);
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.METADATA_RAFT_GROUP_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (CPMemberInfo member : members) {
            out.writeObject(member);
        }
        out.writeLong(membersCommitIndex);
        out.writeInt(groups.size());
        for (CPGroupInfo group : groups) {
            out.writeObject(group);
        }
        out.writeObject(membershipChangeSchedule);
        boolean discoveredInitialCPMembers = initialCPMembers != null;
        out.writeBoolean(discoveredInitialCPMembers);
        if (discoveredInitialCPMembers) {
            out.writeInt(initialCPMembers.size());
            for (CPMemberInfo member : initialCPMembers) {
                out.writeObject(member);
            }
        }
        out.writeInt(initializedCPMembers.size());
        for (CPMemberInfo member : initializedCPMembers) {
            out.writeObject(member);
        }
        out.writeString(initializationStatus.name());
        out.writeInt(initializationCommitIndices.size());
        for (long commitIndex : initializationCommitIndices) {
            out.writeLong(commitIndex);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            CPMemberInfo member = in.readObject();
            members.add(member);
        }
        membersCommitIndex = in.readLong();

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            CPGroupInfo group = in.readObject();
            groups.add(group);
        }
        membershipChangeSchedule = in.readObject();
        boolean discoveredInitialCPMembers = in.readBoolean();
        if (discoveredInitialCPMembers) {
            len = in.readInt();
            initialCPMembers = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                CPMemberInfo member = in.readObject();
                initialCPMembers.add(member);
            }
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            CPMemberInfo member = in.readObject();
            initializedCPMembers.add(member);
        }

        initializationStatus = MetadataRaftGroupInitStatus.valueOf(in.readString());
        len = in.readInt();
        for (int i = 0; i < len; i++) {
            long commitIndex = in.readLong();
            initializationCommitIndices.add(commitIndex);
        }
    }
}
