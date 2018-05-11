package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.proxy.RaftNodeAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.util.Preconditions.checkState;

public class GetInitialRaftGroupMembersIfCurrentGroupMemberOp extends RaftOp implements IdentifiedDataSerializable,
                                                                                        RaftNodeAware {

    private RaftMember raftMember;

    private RaftNode raftNode;

    public GetInitialRaftGroupMembersIfCurrentGroupMemberOp() {
    }

    public GetInitialRaftGroupMembersIfCurrentGroupMemberOp(RaftMember raftMember) {
        this.raftMember = raftMember;
    }

    @Override
    public void setRaftNode(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        checkState(raftNode != null, "RaftNode is not injected in " + groupId);
        Collection<RaftMember> members = raftNode.getCommittedMembers();
        checkState(members.contains(raftMember), raftMember
                + " is not in the current committed member list: " + members + " of " + groupId);
        return new ArrayList<RaftMember>(raftNode.getInitialMembers());
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP;
    }


    @Override
    protected String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(raftMember);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        raftMember = in.readObject();
    }
}
