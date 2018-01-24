package com.hazelcast.raft.impl.service.operation.metadata;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.RaftOp;

public class GetLeavingEndpointContextOp extends RaftOp implements IdentifiedDataSerializable {

    @Override
    protected Object doRun(long commitIndex) {
        RaftService service = getService();
        return service.getMetadataManager().getLeavingEndpointContext();
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
        return RaftServiceDataSerializerHook.GET_LEAVING_ENDPOINT_CONTEXT_OP;
    }

}
