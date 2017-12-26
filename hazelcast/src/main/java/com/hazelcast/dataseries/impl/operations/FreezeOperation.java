package com.hazelcast.dataseries.impl.operations;

import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;

public class FreezeOperation extends DataSeriesOperation {

    public FreezeOperation() {
    }

    public FreezeOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        partition.freeze();
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.FREEZE_OPERATION;
    }
}
