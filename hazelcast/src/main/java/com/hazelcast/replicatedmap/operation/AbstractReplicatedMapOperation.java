package com.hazelcast.replicatedmap.operation;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.spi.Operation;

public abstract class AbstractReplicatedMapOperation
        extends Operation implements DataSerializable {

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

}
