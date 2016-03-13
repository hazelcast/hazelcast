package com.hazelcast.serviceprovider;

import com.hazelcast.spi.RemoteService;
import com.hazelcast.core.DistributedObject;

public class TestRemoteService implements RemoteService {
    public static final String SERVICE_NAME = "TestRemoteService";

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new TestDistributedObject(objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }
}
