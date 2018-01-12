package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;

public class CountOperation extends DataSetOperation {

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
        return DataSetDataSerializerHook.COUNT_OPERATION;
    }
}
