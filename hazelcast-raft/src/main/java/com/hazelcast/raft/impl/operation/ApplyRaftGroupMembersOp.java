package com.hazelcast.raft.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.operation.RaftCommandOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * A {@code RaftCommandOperation} to update members of an existing Raft group. This operation is generated
 * as a result of member add or remove request.
 */
public class ApplyRaftGroupMembersOp extends RaftCommandOperation implements IdentifiedDataSerializable {

    private Collection<RaftEndpoint> members;

    public ApplyRaftGroupMembersOp() {
    }

    public ApplyRaftGroupMembersOp(Collection<RaftEndpoint> members) {
        this.members = members;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPLY_RAFT_GROUP_MEMBERS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(members.size());
        for (RaftEndpoint endpoint : members) {
            out.writeObject(endpoint);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int count = in.readInt();
        members = new LinkedHashSet<RaftEndpoint>();
        for (int i = 0; i < count; i++) {
            RaftEndpoint endpoint = in.readObject();
            members.add(endpoint);
        }
    }

    @Override
    public String toString() {
        return "ApplyRaftGroupMembersOp{" + "members=" + members + '}';
    }
}
