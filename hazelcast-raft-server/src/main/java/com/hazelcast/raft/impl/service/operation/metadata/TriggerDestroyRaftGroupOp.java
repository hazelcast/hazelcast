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

public class TriggerDestroyRaftGroupOp extends RaftOp implements IdentifiedDataSerializable {

    private RaftGroupId targetGroupId;

    public TriggerDestroyRaftGroupOp() {
    }

    public TriggerDestroyRaftGroupOp(RaftGroupId targetGroupId) {
        this.targetGroupId = targetGroupId;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.triggerDestroyRaftGroup(targetGroupId);
        return targetGroupId;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(targetGroupId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        targetGroupId = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.TRIGGER_DESTROY_RAFT_GROUP_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", targetGroupId=").append(targetGroupId);
    }
}
