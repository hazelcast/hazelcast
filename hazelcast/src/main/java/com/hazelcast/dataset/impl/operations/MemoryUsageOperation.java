package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.MemoryInfo;

public class MemoryUsageOperation extends DataSetOperation {

    private MemoryInfo response;

    public MemoryUsageOperation() {
    }

    public MemoryUsageOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        response = partition.memoryInfo();
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.MEMORY_USAGE_OPERATION;
    }
}