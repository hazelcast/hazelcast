package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CompleteDestroyRaftGroupsOp extends RaftOp implements IdentifiedDataSerializable {

    private Set<RaftGroupId> groupIds;

    public CompleteDestroyRaftGroupsOp() {
    }

    public CompleteDestroyRaftGroupsOp(Set<RaftGroupId> groupIds) {
        this.groupIds = groupIds;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.completeDestroyRaftGroups(groupIds);
        return null;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(groupIds.size());
        for (RaftGroupId groupId : groupIds) {
            out.writeObject(groupId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        groupIds = new HashSet<RaftGroupId>();
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
        return RaftServiceDataSerializerHook.COMPLETE_DESTROY_RAFT_GROUPS_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", groupIds=").append(groupIds);
    }
}
