package com.hazelcast.map.impl.operation;

import com.hazelcast.spi.impl.operationservice.Offload;

public abstract class MapNoOffloadOperation extends MapOperation {

    public MapNoOffloadOperation() {
        super();
    }

    public MapNoOffloadOperation(String name) {
        super(name);
    }

    @Override
    public boolean isPendingResult() {
        return false;
    }

    @Override
    protected Offload newIOOperationOffload() {
        return null;
    }

}
