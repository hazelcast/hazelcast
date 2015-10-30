package com.hazelcast.osgi;

import org.osgi.framework.Bundle;
import org.osgi.framework.ServiceReference;

import java.util.concurrent.atomic.AtomicInteger;

class TestServiceReference implements ServiceReference {

    private static AtomicInteger COUNTER = new AtomicInteger();

    private final TestBundle testBundle;
    private final Object service;
    private final Integer id;

    TestServiceReference(TestBundle testBundle, Object service, int id) {
        this.testBundle = testBundle;
        this.service = service;
        this.id = id;
    }

    @Override
    public TestBundle getBundle() {
        return testBundle;
    }

    Object getService() {
        return service;
    }

    @Override
    public Object getProperty(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] getPropertyKeys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bundle[] getUsingBundles() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAssignableTo(Bundle bundle, String className) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Object reference) {
        if (reference instanceof TestServiceReference) {
            return id.compareTo(((TestServiceReference) reference).id);
        }
        return -1;
    }

}
