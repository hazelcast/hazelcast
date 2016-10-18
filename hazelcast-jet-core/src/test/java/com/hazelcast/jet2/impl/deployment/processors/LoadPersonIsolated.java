package com.hazelcast.jet2.impl.deployment.processors;


import com.hazelcast.jet2.Inbox;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import javax.annotation.Nonnull;

import static org.junit.Assert.fail;

public class LoadPersonIsolated implements Processor {

    @Override
    public void init(@Nonnull Outbox outbox) {

    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        while (inbox.poll() != null) {
        }
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
