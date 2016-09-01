package com.hazelcast.jet.impl.job.deployment.processors;

import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;
import org.junit.Assert;

public class PrintCarVertex implements Processor {

    public PrintCarVertex() {
    }

    @Override
    public boolean process(InputChunk input,
                           OutputCollector output,
                           String sourceName, ProcessorContext context) throws Exception {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        contextClassLoader.loadClass("com.sample.pojo.car.Car");
        try {
            contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
            Assert.fail();
        } catch (ClassNotFoundException ignored) {
        }
        output.collect(input);
        return true;
    }
}