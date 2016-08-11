package com.hazelcast.jet.deployment.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.processor.ContainerProcessor;
import java.io.BufferedReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class Apache implements ContainerProcessor {

    public Apache() {
    }

    @Override
    public boolean process(ProducerInputStream inputStream,
                           ConsumerOutputStream outputStream,
                           String sourceName, ProcessorContext processorContext) throws Exception {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL resource = contextClassLoader.getResource("apache");
        BufferedReader reader = Files.newBufferedReader(Paths.get(resource.toURI()));
        reader.readLine();
        String secondLine = reader.readLine();
        assertTrue(secondLine.contains("Apache"));
        assertNull(contextClassLoader.getResource("gpl"));
        outputStream.consumeStream(inputStream);
        return true;
    }
}
