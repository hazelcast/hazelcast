package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;

public class FreezeOperation extends DataSetOperation {

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
        return DataSetDataSerializerHook.FREEZE_OPERATION;
    }
}
