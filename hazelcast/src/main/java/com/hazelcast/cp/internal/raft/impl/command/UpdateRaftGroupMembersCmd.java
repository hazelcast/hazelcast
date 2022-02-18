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

package com.hazelcast.cp.internal.raft.impl.command;

import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.command.RaftGroupCmd;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * A {@code RaftGroupCmd} to update members of an existing Raft group.
 * This command is generated as a result of member add or remove request.
 */
public class UpdateRaftGroupMembersCmd extends RaftGroupCmd implements IdentifiedDataSerializable {

    private Collection<RaftEndpoint> members;
    private RaftEndpoint member;
    private MembershipChangeMode mode;

    public UpdateRaftGroupMembersCmd() {
    }

    public UpdateRaftGroupMembersCmd(Collection<RaftEndpoint> members, RaftEndpoint member, MembershipChangeMode mode) {
        this.members = members;
        this.member = member;
        this.mode = mode;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    public RaftEndpoint getMember() {
        return member;
    }

    public MembershipChangeMode getMode() {
        return mode;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.UPDATE_RAFT_GROUP_MEMBERS_COMMAND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (RaftEndpoint member : members) {
            out.writeObject(member);
        }
        out.writeObject(member);
        out.writeString(mode.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>();
        for (int i = 0; i < count; i++) {
            RaftEndpoint member = in.readObject();
            members.add(member);
        }
        this.members = members;
        this.member = in.readObject();
        this.mode = MembershipChangeMode.valueOf(in.readString());
    }

    @Override
    public String toString() {
        return "UpdateRaftGroupMembersCmd{" + "members=" + members + ", member=" + member + ", mode=" + mode + '}';
    }
}
