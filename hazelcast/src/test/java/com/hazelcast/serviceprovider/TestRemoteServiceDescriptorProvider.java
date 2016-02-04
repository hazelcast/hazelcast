package com.hazelcast.serviceprovider;

import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptorProvider;

public class TestRemoteServiceDescriptorProvider implements RemoteServiceDescriptorProvider {
    private final RemoteServiceDescriptor[] descriptors = new RemoteServiceDescriptor[1];

    @Override
    public RemoteServiceDescriptor[] createRemoteServiceDescriptors() {
        this.descriptors[0] = new TestRemoteServiceDescriptor();
        return this.descriptors;
    }
}
