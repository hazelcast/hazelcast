package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.RaftEndpointImpl;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupOp extends RaftOp implements IdentifiedDataSerializable {

    private String groupName;
    private Collection<RaftEndpointImpl> endpoints;

    public CreateRaftGroupOp() {
    }

    public CreateRaftGroupOp(String groupName, Collection<RaftEndpointImpl> endpoints) {
        this.groupName = groupName;
        this.endpoints = endpoints;
    }

    @Override
    public Object doRun(long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        return metadataManager.createRaftGroup(groupName, endpoints, commitIndex);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(groupName);
        out.writeInt(endpoints.size());
        for (RaftEndpointImpl endpoint : endpoints) {
            out.writeObject(endpoint);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupName = in.readUTF();
        int len = in.readInt();
        endpoints = new ArrayList<RaftEndpointImpl>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpointImpl endpoint = in.readObject();
            endpoints.add(endpoint);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CREATE_RAFT_GROUP_OP;
    }
}
