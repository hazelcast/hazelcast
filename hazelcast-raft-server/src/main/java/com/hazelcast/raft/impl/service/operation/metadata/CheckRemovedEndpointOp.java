package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

public class CheckRemovedEndpointOp extends RaftOperation implements IdentifiedDataSerializable {

    private RaftEndpoint endpoint;

    public CheckRemovedEndpointOp() {
    }

    public CheckRemovedEndpointOp(RaftEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftService service = getService();
        return service.getMetadataManager().isRemoved(endpoint);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(endpoint);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        endpoint = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.CHECK_REMOVED_ENDPOINT_OP;
    }

}
