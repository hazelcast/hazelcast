package com.hazelcast.partition.impl;

import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.AbstractOperation;

public class AssignPartitions extends AbstractOperation {

    @Override
    public void run() {
        InternalPartitionServiceImpl service = getService();
        service.firstArrangement();
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }
}
