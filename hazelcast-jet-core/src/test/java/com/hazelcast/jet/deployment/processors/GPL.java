package com.hazelcast.jet.deployment.processors;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.processor.ContainerProcessor;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GPL implements ContainerProcessor {

    public GPL() {
    }

    @Override
    public boolean process(ProducerInputStream inputStream,
                           ConsumerOutputStream outputStream,
                           String sourceName, ProcessorContext processorContext) throws Exception {

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        URL resource = contextClassLoader.getResource("gpl");
        String firstLine = Files.newBufferedReader(Paths.get(resource.toURI())).readLine();
        assertTrue(firstLine.contains("GNU"));
        assertNull(contextClassLoader.getResource("apache"));
        outputStream.consumeStream(inputStream);
        return true;
    }
}
