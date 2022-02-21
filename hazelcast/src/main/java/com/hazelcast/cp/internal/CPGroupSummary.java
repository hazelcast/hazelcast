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

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class CPGroupSummary implements CPGroup, IdentifiedDataSerializable {

    private CPGroupId id;
    private CPGroupStatus status;
    private Set<RaftEndpoint> initialMembers;
    private Set<CPMember> members;

    public CPGroupSummary() {
    }

    public CPGroupSummary(CPGroupId id, CPGroupStatus status, Collection<RaftEndpoint> initialMembers,
                          Collection<CPMember> members) {
        this.id = id;
        this.status = status;
        this.initialMembers = unmodifiableSet(new LinkedHashSet<>(initialMembers));
        this.members = unmodifiableSet(new LinkedHashSet<>(members));
    }

    @Override
    public CPGroupId id() {
        return id;
    }

    @Override
    public CPGroupStatus status() {
        return status;
    }

    public Collection<RaftEndpoint> initialMembers() {
        return initialMembers;
    }

    @Override
    public Collection<CPMember> members() {
        return members;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.CP_GROUP_SUMMARY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(id);
        out.writeInt(initialMembers.size());
        for (RaftEndpoint member : initialMembers) {
            out.writeObject(member);
        }
        out.writeInt(members.size());
        for (CPMember member : members) {
            out.writeObject(member);
        }
        out.writeString(status.toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readObject();
        int initialMemberCount = in.readInt();
        Set<RaftEndpoint> initialMembers = new LinkedHashSet<RaftEndpoint>();
        for (int i = 0; i < initialMemberCount; i++) {
            RaftEndpoint member = in.readObject();
            initialMembers.add(member);
        }
        this.initialMembers = unmodifiableSet(initialMembers);

        int memberCount = in.readInt();
        members = new LinkedHashSet<CPMember>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            CPMember member = in.readObject();
            members.add(member);
        }
        members = unmodifiableSet(members);
        status = CPGroupStatus.valueOf(in.readString());
    }

    @Override
    public String toString() {
        return "CPGroup{" + "id=" + id + ", status=" + status + ", initialMembers=" + initialMembers + ", members="
                + members + '}';
    }
}
