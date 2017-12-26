package com.hazelcast.dataseries.impl.operations;

import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;

public class CountOperation extends DataSeriesOperation {

    private long count;

    public CountOperation() {
    }

    public CountOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        count = partition.count();
    }

    @Override
    public Object getResponse() {
        return count;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.COUNT_OPERATION;
    }
}
