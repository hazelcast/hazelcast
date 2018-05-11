package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftSystemOperation;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class CreateRaftNodeOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    private RaftGroupId groupId;
    private Collection<RaftMember> initialMembers;

    public CreateRaftNodeOp() {
    }

    public CreateRaftNodeOp(RaftGroupId groupId, Collection<RaftMember> initialMembers) {
        this.groupId = groupId;
        this.initialMembers = initialMembers;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.createRaftNode(groupId, initialMembers);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeInt(initialMembers.size());
        for (RaftMember member : initialMembers) {
            out.writeObject(member);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        int count = in.readInt();
        initialMembers = new ArrayList<RaftMember>(count);
        for (int i = 0; i < count; i++) {
            RaftMember member = in.readObject();
            initialMembers.add(member);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_NODE_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupId=").append(groupId).append(", initialMembers=").append(initialMembers);
    }
}
