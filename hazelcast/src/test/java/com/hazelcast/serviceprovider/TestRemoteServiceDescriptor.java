package com.hazelcast.serviceprovider;


import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptor;

public class TestRemoteServiceDescriptor implements RemoteServiceDescriptor {
    @Override
    public String getServiceName() {
        return TestRemoteService.SERVICE_NAME;
    }

    @Override
    public RemoteService getService(NodeEngine nodeEngine) {
        return new TestRemoteService();
    }
}
