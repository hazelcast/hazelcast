package com.hazelcast.jet.impl.job.deployment.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.processor.Processor;

import static org.junit.Assert.fail;

public class PrintPersonVertex implements Processor {

    public PrintPersonVertex() {
    }

    @Override
    public boolean process(InputChunk input,
                           OutputCollector output,
                           String sourceName, ProcessorContext context) throws Exception {

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
