package com.hazelcast.jet2.impl.deployment.processors;

import com.hazelcast.jet2.Inbox;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.annotation.Nonnull;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ApacheV1Isolated implements Processor {

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
        URL resource = contextClassLoader.getResource("apachev1");
        assertNotNull(resource);
        BufferedReader reader = null;
        try {
            reader = Files.newBufferedReader(Paths.get(resource.toURI()));
            String firstLine = reader.readLine();
            String secondLine = reader.readLine();
            assertTrue(secondLine.contains("Version 1.1"));
            assertNull(contextClassLoader.getResourceAsStream("apachev2"));

        } catch (IOException | URISyntaxException e) {
            fail();
        }
        return true;

    }
}
