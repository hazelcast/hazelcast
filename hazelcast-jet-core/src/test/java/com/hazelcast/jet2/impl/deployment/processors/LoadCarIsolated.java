package com.hazelcast.jet2.impl.deployment.processors;

import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import javax.annotation.Nonnull;

import static org.junit.Assert.fail;

public class LoadCarIsolated implements Processor {

    public LoadCarIsolated() {
    }

    @Override
    public void init(@Nonnull Outbox outbox) {

    }

    @Override
    public boolean process(int ordinal, Object item) {
        return true;
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
        return true;    }
}