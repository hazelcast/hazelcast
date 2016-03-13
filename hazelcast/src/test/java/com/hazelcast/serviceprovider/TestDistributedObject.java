package com.hazelcast.serviceprovider;

import com.hazelcast.core.DistributedObject;

public class TestDistributedObject implements DistributedObject {
    private final String objectName;

    public TestDistributedObject(String objectName) {
        this.objectName = objectName;
    }

    @Override
    public String getPartitionKey() {
        return "test";
    }

    @Override
    public String getName() {
        return this.objectName;
    }

    @Override
    public String getServiceName() {
        return TestRemoteService.SERVICE_NAME;
    }

    @Override
    public void destroy() {

    }
}
