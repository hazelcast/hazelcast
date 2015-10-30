package com.hazelcast.osgi;

import org.osgi.framework.ServiceRegistration;

import java.util.Dictionary;

class TestServiceRegistration implements ServiceRegistration {

    private TestServiceReference testServiceReference;

    TestServiceRegistration(TestServiceReference testServiceReference) {
        this.testServiceReference = testServiceReference;
    }

    @Override
    public TestServiceReference getReference() {
        return testServiceReference;
    }

    @Override
    public void unregister() {
        testServiceReference = null;
    }

    @Override
    public void setProperties(Dictionary properties) {
        throw new UnsupportedOperationException();
    }

}
