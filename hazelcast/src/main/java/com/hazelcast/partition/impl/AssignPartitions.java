package com.hazelcast.partition.impl;

import com.hazelcast.spi.AbstractOperation;

public class AssignPartitions extends AbstractOperation {

    @Override
    public void run() {
        InternalPartitionServiceImpl service = getService();
        service.firstArrangement();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }
}
