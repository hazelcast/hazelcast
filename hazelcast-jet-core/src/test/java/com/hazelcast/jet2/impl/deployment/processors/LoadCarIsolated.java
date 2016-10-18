package com.hazelcast.jet2.impl.deployment.processors;

import com.hazelcast.jet2.impl.AbstractProcessor;

import static org.junit.Assert.fail;

public class LoadCarIsolated extends AbstractProcessor {

    public LoadCarIsolated() {
    }

    @Override
    public boolean complete() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            contextClassLoader.loadClass("com.sample.pojo.car.Car");
        } catch (ClassNotFoundException e) {
            fail();
        }
        try {
            contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
            fail();
        } catch (ClassNotFoundException ignored) {
        }
        return true;
    }
}