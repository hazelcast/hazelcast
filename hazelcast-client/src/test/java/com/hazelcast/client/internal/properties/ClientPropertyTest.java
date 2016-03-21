package com.hazelcast.client.internal.properties;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientPropertyTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(ClientProperty.class);
    }

    @Test
    public void testGetPropertyFromHazelcastClientInstance() {
        String propertyKey = ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName();

        hazelcastFactory.newHazelcastInstance();

        ClientConfig config = new ClientConfig();
        config.setProperty(propertyKey, "notDefaultValue");
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);

        assertEquals("notDefaultValue", client.getProperty(propertyKey));
    }
}
