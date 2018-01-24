package com.hazelcast.raft.impl.command;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.command.RaftGroupCmd;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * A {@code RaftGroupCmd} to update members of an existing Raft group.
 * This command is generated as a result of member add or remove request.
 */
public class ApplyRaftGroupMembersCmd extends RaftGroupCmd implements IdentifiedDataSerializable {

    private Collection<RaftEndpoint> members;

    public ApplyRaftGroupMembersCmd() {
    }

    public ApplyRaftGroupMembersCmd(Collection<RaftEndpoint> members) {
        this.members = members;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Override
    public String toString() {
        return "ApplyRaftGroupMembersCmd{" + "members=" + members + '}';
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPLY_RAFT_GROUP_MEMBERS_COMMAND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.size());
        for (RaftEndpoint endpoint : members) {
            out.writeObject(endpoint);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        Collection<RaftEndpoint> members = new LinkedHashSet<RaftEndpoint>();
        for (int i = 0; i < count; i++) {
            RaftEndpoint endpoint = in.readObject();
            members.add(endpoint);
        }
        this.members = members;
    }
}
