package com.hazelcast.dataseries.impl.operations;

import com.hazelcast.dataseries.MemoryInfo;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;


public class MemoryUsageOperation extends DataSeriesOperation {

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
        return DataSeriesDataSerializerHook.MEMORY_USAGE_OPERATION;
    }
}