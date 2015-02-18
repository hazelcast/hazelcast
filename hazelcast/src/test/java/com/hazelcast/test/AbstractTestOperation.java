package com.hazelcast.test;

import com.hazelcast.spi.AbstractOperation;

public abstract class AbstractTestOperation extends AbstractOperation {
    public static final Object NO_RESPONSE = new Object() {
        @Override
        public String toString() {
            return "NO_RESPONSE";
        }
    };

    private volatile Object response = NO_RESPONSE;

    public AbstractTestOperation(int partitionId) {
        setPartitionId(partitionId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    public boolean hasResponse(){
        return response!=NO_RESPONSE;
    }

    @Override
    public void run() throws Exception {
        response = doRun();
    }


    protected abstract Object doRun();
}
