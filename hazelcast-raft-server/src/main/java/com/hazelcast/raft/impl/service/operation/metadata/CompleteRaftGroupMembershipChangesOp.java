package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.util.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CompleteRaftGroupMembershipChangesOp extends RaftOp implements IdentifiedDataSerializable {

    private Map<RaftGroupId, Tuple2<Long, Long>> changedGroups;

    public CompleteRaftGroupMembershipChangesOp() {
    }

    public CompleteRaftGroupMembershipChangesOp(Map<RaftGroupId, Tuple2<Long, Long>> changedGroups) {
        this.changedGroups = changedGroups;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        return metadataManager.completeRaftGroupMembershipChanges(changedGroups);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(changedGroups.size());
        for (Entry<RaftGroupId, Tuple2<Long, Long>> e : changedGroups.entrySet()) {
            out.writeObject(e.getKey());
            Tuple2<Long, Long> value = e.getValue();
            out.writeLong(value.element1);
            out.writeLong(value.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        changedGroups = new HashMap<RaftGroupId, Tuple2<Long, Long>>(count);
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            long currMembersCommitIndex = in.readLong();
            long newMembersCommitIndex = in.readLong();
            changedGroups.put(groupId, Tuple2.of(currMembersCommitIndex, newMembersCommitIndex));
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", changedGroups=").append(changedGroups);
    }
}
