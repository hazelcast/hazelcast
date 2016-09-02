package com.hazelcast.jet.impl.job.deployment.processors;

import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;
import com.hazelcast.jet.Processor;

import java.io.BufferedReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ApacheV2 implements Processor {

    public ApacheV2() {
    }

    @Override
    public boolean process(InputChunk input,
                           OutputCollector output,
                           String sourceName) throws Exception {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL resource = contextClassLoader.getResource("apachev2");
        BufferedReader reader = Files.newBufferedReader(Paths.get(resource.toURI()));
        String firstLine = reader.readLine();
        String secondLine = reader.readLine();
        assertTrue(secondLine.contains("Apache"));
        assertNull(contextClassLoader.getResourceAsStream("apachev1"));
        output.collect(input);
        return true;
    }
}
