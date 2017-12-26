package com.hazelcast.dataseries.impl.operations;

import java.util.Iterator;

import static com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook.ITERATOR_OPERATION;

public class IteratorOperation extends DataSeriesOperation {

    private transient Iterator result;

    public IteratorOperation() {
    }

    public IteratorOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        result = partition.iterator();
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getId() {
        return ITERATOR_OPERATION;
    }
}
