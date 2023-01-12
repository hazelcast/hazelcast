/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;

/**
 * Snapshot of the Metadata Raft group state
 * This class should be IMMUTABLE since it is stored in the RaftLog's SnapshotEntry
 */
public final class MetadataRaftGroupSnapshot implements IdentifiedDataSerializable {

    private final Collection<CPMemberInfo> members = new ArrayList<>();
    private long membersCommitIndex;
    private final Collection<CPGroupInfo> groups = new ArrayList<>();
    private MembershipChangeSchedule membershipChangeSchedule;
    private List<CPMemberInfo> initialCPMembers;
    private final Set<CPMemberInfo> initializedCPMembers = new HashSet<>();
    private MetadataRaftGroupInitStatus initializationStatus;
    private final Set<Long> initializationCommitIndices = new HashSet<>();

    public MetadataRaftGroupSnapshot() {
    }

    public MetadataRaftGroupSnapshot(Collection<CPMemberInfo> members,
                                     long membersCommitIndex,
                                     Collection<CPGroupInfo> groups,
                                     MembershipChangeSchedule membershipChangeSchedule,
                                     List<CPMemberInfo> initialCPMembers,
                                     Set<CPMemberInfo> initializedCPMembers,
                                     MetadataRaftGroupInitStatus initializationStatus,
                                     Set<Long> initializationCommitIndices) {
        this.members.addAll(members);
        this.membersCommitIndex = membersCommitIndex;
        // Deep copy CPGroupInfo, because it's a mutable object.
        groups.stream().map(CPGroupInfo::new).forEach(this.groups::add);
        this.membershipChangeSchedule = membershipChangeSchedule;
        this.initialCPMembers = initialCPMembers;
        this.initializedCPMembers.addAll(initializedCPMembers);
        this.initializationStatus = initializationStatus;
        this.initializationCommitIndices.addAll(initializationCommitIndices);
    }

    public Collection<CPMemberInfo> getMembers() {
        return unmodifiableCollection(members);
    }

    public long getMembersCommitIndex() {
        return membersCommitIndex;
    }

    public Collection<CPGroupInfo> getGroups() {
        // Deep copy CPGroupInfo, because it's a mutable object.
        return groups.stream().map(CPGroupInfo::new).collect(Collectors.toList());
    }

    public MembershipChangeSchedule getMembershipChangeSchedule() {
        return membershipChangeSchedule;
    }

    public Set<CPMemberInfo> getInitializedCPMembers() {
        return unmodifiableSet(initializedCPMembers);
    }

    public List<CPMemberInfo> getInitialCPMembers() {
        return unmodifiableList(initialCPMembers);
    }

    public MetadataRaftGroupInitStatus getInitializationStatus() {
        return initializationStatus;
    }

    public Set<Long> getInitializationCommitIndices() {
        return unmodifiableSet(initializationCommitIndices);
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

    @Override
    public String toString() {
        return "MetadataRaftGroupSnapshot{"
                + "members=" + members
                + ", membersCommitIndex=" + membersCommitIndex
                + ", groups=" + groups
                + ", membershipChangeSchedule=" + membershipChangeSchedule
                + ", initialCPMembers=" + initialCPMembers
                + ", initializedCPMembers=" + initializedCPMembers
                + ", initializationStatus=" + initializationStatus
                + ", initializationCommitIndices=" + initializationCommitIndices
                + '}';
    }
}
