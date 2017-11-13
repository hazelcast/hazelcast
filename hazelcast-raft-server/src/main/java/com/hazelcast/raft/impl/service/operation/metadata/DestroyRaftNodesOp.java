package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class DestroyRaftNodesOp extends Operation implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private Collection<RaftGroupId> groupIds;

    public DestroyRaftNodesOp() {
    }

    public DestroyRaftNodesOp(Collection<RaftGroupId> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    public void run() {
        RaftService service = getService();
        for (RaftGroupId groupId : groupIds) {
            service.destroyRaftNode(groupId);
        }
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
        out.writeInt(groupIds.size());
        for (RaftGroupId groupId : groupIds) {
            out.writeObject(groupId);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int count = in.readInt();
        groupIds = new ArrayList<RaftGroupId>();
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            groupIds.add(groupId);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.DESTROY_RAFT_NODES_OP;
    }
}
