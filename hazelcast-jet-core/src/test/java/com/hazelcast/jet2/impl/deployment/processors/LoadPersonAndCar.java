package com.hazelcast.jet2.impl.deployment.processors;

import com.hazelcast.jet2.impl.AbstractProcessor;

import static org.junit.Assert.fail;

public class LoadPersonAndCar extends AbstractProcessor {

    public LoadPersonAndCar() {
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
        } catch (ClassNotFoundException ignored) {
            fail();
        }
        return true;
    }
}