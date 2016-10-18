package com.hazelcast.jet2.impl.deployment.processors;


import com.hazelcast.jet2.impl.AbstractProcessor;

import static org.junit.Assert.fail;

public class LoadPersonIsolated extends AbstractProcessor {

    public LoadPersonIsolated() {
    }

    @Override
    public boolean complete() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        } catch (ClassNotFoundException e) {
            fail(e.getMessage());
        }
        try {
            contextClassLoader.loadClass("com.sample.pojo.car.Car");
            fail();
        } catch (ClassNotFoundException ignored) {
        }
        return true;
    }
}
