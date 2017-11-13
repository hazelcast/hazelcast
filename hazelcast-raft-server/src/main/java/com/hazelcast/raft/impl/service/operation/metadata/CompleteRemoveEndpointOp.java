package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.util.Pair;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CompleteRemoveEndpointOp extends RaftOperation implements IdentifiedDataSerializable {

    private RaftEndpoint endpoint;

    private Map<RaftGroupId, Pair<Long, Long>> leftGroups;

    public CompleteRemoveEndpointOp() {
    }

    public CompleteRemoveEndpointOp(RaftEndpoint endpoint, Map<RaftGroupId, Pair<Long, Long>> leftGroups) {
        this.endpoint = endpoint;
        this.leftGroups = leftGroups;
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        metadataManager.completeRemoveEndpoint(endpoint, leftGroups);
        return endpoint;
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(endpoint);
        out.writeInt(leftGroups.size());
        for (Entry<RaftGroupId, Pair<Long, Long>> e : leftGroups.entrySet()) {
            out.writeObject(e.getKey());
            Pair<Long, Long> value = e.getValue();
            out.writeLong(value.getPrimary());
            out.writeLong(value.getSecondary());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        endpoint = in.readObject();
        int count = in.readInt();
        leftGroups = new HashMap<RaftGroupId, Pair<Long, Long>>(count);
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            long currMembersCommitIndex = in.readLong();
            long newMembersCommitIndex = in.readLong();
            leftGroups.put(groupId, new Pair<Long, Long>(currMembersCommitIndex, newMembersCommitIndex));
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.COMPLETE_REMOVE_ENDPOINT_OP;
    }
}
