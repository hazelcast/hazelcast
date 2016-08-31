package com.hazelcast.jet.impl.job.deployment.processors;

import com.hazelcast.jet.data.io.OutputCollector;
import com.hazelcast.jet.data.io.InputChunk;
import com.hazelcast.jet.processor.Processor;
import com.hazelcast.jet.processor.ProcessorContext;
import java.io.BufferedReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ApacheV1 implements Processor {

    public ApacheV1() {
    }

    @Override
    public boolean process(InputChunk input,
                           OutputCollector output,
                           String sourceName, ProcessorContext context) throws Exception {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL resource = contextClassLoader.getResource("apachev1");
        BufferedReader reader = Files.newBufferedReader(Paths.get(resource.toURI()));
        String firstLine = reader.readLine();
        String secondLine = reader.readLine();
        assertTrue(secondLine.contains("Version 1.1"));
        assertNull(contextClassLoader.getResourceAsStream("apachev2"));
        output.collect(input);
        return true;
    }
}
