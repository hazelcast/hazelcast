package com.hazelcast.jet.impl.job.deployment.processors;

import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.Processor;

import static org.junit.Assert.fail;

public class PrintPersonVertex implements Processor {

    public PrintPersonVertex() {
    }

    @Override
    public boolean process(InputChunk input,
                           OutputCollector output,
                           String sourceName) throws Exception {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        try {
            contextClassLoader.loadClass("com.sample.pojo.car.Car");
            fail();
        } catch (ClassNotFoundException ignored) {
        }
        output.collect(input);
        return true;
    }
}
