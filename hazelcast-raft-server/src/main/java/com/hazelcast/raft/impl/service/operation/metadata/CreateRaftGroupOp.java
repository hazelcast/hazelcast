package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftMetadataManager;
import com.hazelcast.raft.operation.RaftOperation;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 */
public class CreateRaftGroupOp extends RaftOperation implements IdentifiedDataSerializable {

    private String serviceName;
    private String name;
    private Collection<RaftEndpoint> endpoints;

    public CreateRaftGroupOp() {
    }

    public CreateRaftGroupOp(String serviceName, String name, Collection<RaftEndpoint> endpoints) {
        this.serviceName = serviceName;
        this.name = name;
        this.endpoints = endpoints;
    }

    @Override
    public Object doRun(long commitIndex) {
        RaftService service = getService();
        RaftMetadataManager metadataManager = service.getMetadataManager();
        return metadataManager.createRaftGroup(serviceName, name, endpoints, commitIndex);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(serviceName);
        out.writeUTF(name);
        out.writeInt(endpoints.size());
        for (RaftEndpoint endpoint : endpoints) {
            out.writeObject(endpoint);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readUTF();
        name = in.readUTF();
        int len = in.readInt();
        endpoints = new ArrayList<RaftEndpoint>(len);
        for (int i = 0; i < len; i++) {
            RaftEndpoint endpoint = in.readObject();
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
