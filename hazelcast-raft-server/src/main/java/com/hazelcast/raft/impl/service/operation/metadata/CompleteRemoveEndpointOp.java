package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.util.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CompleteRemoveEndpointOp extends RaftOp implements IdentifiedDataSerializable {

    private RaftEndpointImpl endpoint;

    private Map<RaftGroupId, Tuple2<Long, Long>> leftGroups;

    public CompleteRemoveEndpointOp() {
    }

    public CompleteRemoveEndpointOp(RaftEndpointImpl endpoint, Map<RaftGroupId, Tuple2<Long, Long>> leftGroups) {
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
        for (Entry<RaftGroupId, Tuple2<Long, Long>> e : leftGroups.entrySet()) {
            out.writeObject(e.getKey());
            Tuple2<Long, Long> value = e.getValue();
            out.writeLong(value.element1);
            out.writeLong(value.element2);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        endpoint = in.readObject();
        int count = in.readInt();
        leftGroups = new HashMap<RaftGroupId, Tuple2<Long, Long>>(count);
        for (int i = 0; i < count; i++) {
            RaftGroupId groupId = in.readObject();
            long currMembersCommitIndex = in.readLong();
            long newMembersCommitIndex = in.readLong();
            leftGroups.put(groupId, Tuple2.of(currMembersCommitIndex, newMembersCommitIndex));
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
