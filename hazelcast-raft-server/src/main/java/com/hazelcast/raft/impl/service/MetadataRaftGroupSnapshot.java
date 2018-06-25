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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftMemberImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Snapshot of the Metadata Raft group state
 */
public final class MetadataRaftGroupSnapshot implements IdentifiedDataSerializable {

    private final Collection<RaftMemberImpl> members = new ArrayList<RaftMemberImpl>();
    private final Collection<RaftGroupInfo> raftGroups = new ArrayList<RaftGroupInfo>();
    private MembershipChangeContext membershipChangeContext;

    public void addRaftGroup(RaftGroupInfo group) {
        raftGroups.add(group);
    }

    public void addMember(RaftMemberImpl member) {
        members.add(member);
    }

    public Collection<RaftMemberImpl> getMembers() {
        return members;
    }

    public Collection<RaftGroupInfo> getRaftGroups() {
        return raftGroups;
    }

    public MembershipChangeContext getMembershipChangeContext() {
        return membershipChangeContext;
    }

    public void setMembershipChangeContext(MembershipChangeContext membershipChangeContext) {
        this.membershipChangeContext = membershipChangeContext;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.METADATA_RAFT_GROUP_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (RaftMemberImpl member : members) {
            out.writeObject(member);
        }
        out.writeInt(raftGroups.size());
        for (RaftGroupInfo group : raftGroups) {
            out.writeObject(group);
        }
        out.writeObject(membershipChangeContext);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftMemberImpl member = in.readObject();
            members.add(member);
        }

        len = in.readInt();
        for (int i = 0; i < len; i++) {
            RaftGroupInfo group = in.readObject();
            raftGroups.add(group);
        }
        membershipChangeContext = in.readObject();
    }
}
