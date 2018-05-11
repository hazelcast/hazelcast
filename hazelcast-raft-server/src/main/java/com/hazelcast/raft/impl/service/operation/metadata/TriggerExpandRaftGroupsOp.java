package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TriggerExpandRaftGroupsOp extends RaftOp implements IdentifiedDataSerializable {

    private Map<RaftGroupId, RaftMemberImpl> membersToAdd;

    public TriggerExpandRaftGroupsOp() {
    }

    public TriggerExpandRaftGroupsOp(Map<RaftGroupId, RaftMemberImpl> membersToAdd) {
        this.membersToAdd = membersToAdd;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        return metadataManager.triggerExpandRaftGroups(membersToAdd);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.TRIGGER_EXPAND_RAFT_GROUPS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(membersToAdd.size());
        for (Entry<RaftGroupId, RaftMemberImpl> e : membersToAdd.entrySet()) {
            out.writeObject(e.getKey());
            out.writeObject(e.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        membersToAdd = new HashMap<RaftGroupId, RaftMemberImpl>();
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            RaftMemberImpl member = in.readObject();
            membersToAdd.put(groupId, member);
        }
    }
}
